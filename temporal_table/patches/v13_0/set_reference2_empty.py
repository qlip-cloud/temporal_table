from __future__ import unicode_literals
import frappe

def execute():

    frappe.db.sql(
        """UPDATE `tabqp_tmp_sales_orders`
        SET `reference_2` = ''
        WHERE `reference_2` IS NULL""")

    frappe.db.sql(
        """UPDATE `tabSales Order`
        SET `qp_reference2` = ''
        WHERE `qp_reference2` IS NULL""")
