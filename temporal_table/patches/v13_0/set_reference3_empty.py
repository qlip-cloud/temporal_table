from __future__ import unicode_literals
import frappe

def execute():

    frappe.db.sql(
        """UPDATE `tabqp_tmp_sales_orders`
        SET `reference_3` = ''
        WHERE `reference_3` IS NULL""")

    frappe.db.sql(
        """UPDATE `tabSales Order`
        SET `qp_reference3` = ''
        WHERE `qp_reference3` IS NULL""")
