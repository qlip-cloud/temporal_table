import frappe

def add_index(doc=None, method=None):
    frappe.db.add_index("Sales Order", ["qp_reference1"])
    frappe.db.add_index("Sales Order", ["qp_reference2"])
    frappe.db.add_index("Sales Order", ["qp_reference1_old"])
    frappe.db.add_index("Sales Order", ["qp_reference2_old"])
    frappe.db.add_index("Sales Order", ["qp_year_week"])
    frappe.db.add_index("Sales Order", ["qp_category"])
    frappe.db.add_index("Sales Order", ["qp_origin_process"])
    frappe.db.add_index("Sales Order", ["is_updated"])

    # Garantizar que se aplique donde se use qp_reference1
    res = frappe.db.sql(
        """SELECT qp_reference1 FROM `tabSales Order`
        WHERE qp_reference1 is not null LIMIT 1""")

    if res:

        res = frappe.db.sql(
            """SELECT qp_reference1_old FROM `tabSales Order`
            WHERE qp_reference1_old IS NOT NULL LIMIT 1""")

        if not res:
            print("Se actualiza qp_reference1_old y qp_reference2_old en Sales Order")
            frappe.db.sql(
                """UPDATE `tabSales Order`
                SET `qp_reference1_old` = qp_reference1
                WHERE `qp_reference1_old` IS NULL""")

            frappe.db.sql(
                """UPDATE `tabSales Order`
                SET `qp_reference2_old` = qp_reference2
                WHERE `qp_reference2_old` IS NULL""")
