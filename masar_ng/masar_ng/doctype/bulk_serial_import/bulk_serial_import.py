import csv
from frappe.model.document import Document
from frappe.utils.background_jobs import enqueue
from frappe import _, frappe

class BulkSerialImport(Document):
    def on_submit(self):
        self.enqueue_bulk_processing()

    def enqueue_bulk_processing(self):
        print("Enqueuing bulk serial import job...")  # Debugging
        try:
            enqueue(
                process_bulk_serial_import,
                queue='long',
                timeout=7200,
                job_id=f"bulk_serial_import_{self.name}",
                doctype="Bulk Serial Import",
                docname=self.name
            )
            print("Job enqueued successfully!")  # Debugging
        except Exception as e:
            print(f"Failed to enqueue job: {e}")  # Debugging
            frappe.throw(_("Failed to enqueue job. Please check logs for details."))
        frappe.msgprint(_("Bulk serial processing started in background. Refresh page later to see progress."))

def process_bulk_serial_import(doctype, docname):
    doc = frappe.get_doc(doctype, docname)
    try:
        # 1. Create Purchase Receipt
        pr = create_purchase_receipt(doc.purchase_order, doc.warehouse)
        
        # 2. Process CSV
        file_path = frappe.get_doc("File", {"file_url": doc.csv_file}).get_full_path()
        
        # 3. Process serials
        process_serial_csv(
            file_path=file_path,
            item_code=pr.items[0].item_code,
            warehouse=doc.warehouse,
            pr_name=pr.name,
            pr_item_row=pr.items[0].name
        )
        
        # 4. Submit PR
        pr.reload()
        pr.submit()
        
        doc.db_set("status", "Completed")
        frappe.db.commit()

    except Exception as e:
        frappe.db.rollback()
        doc.db_set("status", "Failed")
        frappe.log_error(_("Bulk Serial Import Failed"), reference_doctype=doctype, reference_name=docname)
        raise

def create_purchase_receipt(po_name, warehouse):
    po = frappe.get_doc("Purchase Order", po_name)
    
    pr = frappe.new_doc("Purchase Receipt")
    pr.update({
        "supplier": po.supplier,
        "company": po.company,
        "purchase_order": po.name,
        "items": [{
            "item_code": po.items[0].item_code,
            "qty": po.items[0].qty,
            "uom": po.items[0].uom,
            "warehouse": warehouse,  # Use selected warehouse
            "purchase_order_item": po.items[0].name
        }]
    })
    
    pr.insert(ignore_permissions=True)
    pr.save()
    return pr

def process_serial_csv(file_path, item_code, warehouse, pr_name, pr_item_row):
    chunk_size = 100000  # Process 100,000 serials at a time
    
    try:
        with open(file_path, 'r') as f:
            csv_reader = csv.reader(f)
            headers = next(csv_reader)  # Skip header row
            
            # Create bundle
            bundle = frappe.new_doc("Serial and Batch Bundle")
            bundle.update({
                "item_code": item_code,
                "warehouse": warehouse,
                "type_of_transaction": "Inward",
                "voucher_type": "Purchase Receipt",
                "voucher_no": pr_name,
                "voucher_detail_no": pr_item_row
            })
            
            # Process serials in chunks
            serials = []
            for row in csv_reader:
                if row:
                    serial_no = row[0].strip()
                    if serial_no:  # Ensure serial is not empty
                        serials.append(serial_no)
                    
                    if len(serials) >= chunk_size:
                        process_serial_batch(serials, item_code, warehouse, bundle)
                        frappe.db.commit()
                        serials = []  # Reset for next chunk
            
            # Process remaining serials
            if serials:
                process_serial_batch(serials, item_code, warehouse, bundle)
                frappe.db.commit()
                
            # Save bundle after all serials are added
            bundle.insert(ignore_permissions=True)
            frappe.db.commit()
                
    except Exception as e:
        frappe.db.rollback()
        raise

def process_serial_batch(serials, item_code, warehouse, bundle):
    # Ensure bundle.total_qty is initialized to 0 if it is None
    if bundle.total_qty is None:
        bundle.total_qty = 0

    # 1. Create missing serials without warehouse (bulk insert)
    existing = frappe.get_all("Serial No", 
        filters={"name": ["in", serials]},
        pluck="name"
    )
    new_serials = list(set(serials) - set(existing))
    
    if new_serials:
        # Prepare data for bulk insert
        fields = ["name", "serial_no", "item_code", "status"]  # Fields to insert
        serial_data = [
            (sn, sn, item_code, "Inactive")  # Tuple of values
            for sn in new_serials
        ]
        
        # Perform bulk insert
        frappe.db.bulk_insert("Serial No", fields=fields, values=serial_data, ignore_duplicates=True)
    
    # 2. Add to bundle with warehouse (optimized)
    for sn in serials:
        bundle.append("entries", {
            "serial_no": sn,
            "qty": 1,  # Always set quantity to 1 for each serial
            "warehouse": warehouse,
            "incoming_rate": 0
        })
    
    # 3. Update bundle totals
    bundle.total_qty += len(serials)  # Add the number of serials to the total quantity
    if bundle.avg_rate:
        bundle.total_amount += len(serials) * bundle.avg_rate  # Update total amount if avg_rate is set