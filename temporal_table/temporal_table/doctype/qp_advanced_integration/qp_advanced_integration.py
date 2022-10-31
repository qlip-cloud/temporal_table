# Copyright (c) 2022, Mentum and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe import _

import os

from temporal_table.temporal_table.use_case.import_journal_entry import import_je
from temporal_table.temporal_table.use_case.import_sales_order import import_tso



class qp_Advanced_Integration(Document):

	def validate(self):

		if self.import_type == "qp_je":
			if not self.import_file:
				frappe.throw(_("Please attach file to import"))

			root, extension = os.path.splitext(self.import_file)
			if extension != ".csv":
				frappe.throw(_("Allowed extension .csv"))


	@frappe.whitelist()
	def journal_entry_import(self):

		try:

			result_import = self.do_import()

			return "okay"


		except Exception as error:

			frappe.log_error(message=frappe.get_traceback(), title="journal_entry_import")

			pass

		return "error"

	def do_import(self):

		# Validar que haya un adjunto
		if not self.import_file or not self.company:
			frappe.throw(_("Please attach file to import or select company"))

		# Validar la extensión del archivo para la carga de sale ordes
		if self.import_type == "qp_tso":
			root, extension = os.path.splitext(self.import_file)
			if extension not in (".xlsx", ".xls"):
				frappe.throw(_("Allowed extension .xlsx or .xls"))

		# Validar que haya un tipo de diario selecionado si se va a importar un Journal Entry
		if self.import_type == "qp_je" and not self.journal_type:
			frappe.throw(_("Please select Journal Type."))

		if self.status == 'Active':
			frappe.throw(_("Background job already running."))

		# validar procesos simultáneos
		simultaneous_process = frappe.db.sql("""
				Select count(*)
				from tabqp_Advanced_Integration
				WHERE status = 'Active' and import_type = 'qp_je'""")[0][0] or 0

		if simultaneous_process != 0:
			frappe.throw(_("Other background job of Journal Entry already running."))
		
		self.status = 'Active'
		self.save(ignore_permissions=True)
		self.reload()

		if self.import_type == "qp_je":

			frappe.enqueue(import_je, doc=self, queue='long', is_async=True, timeout=54000)

			frappe.msgprint(_('Background Job Created. Wait for the result'))

		elif self.import_type == "qp_tso":

			frappe.enqueue(import_tso, doc=self, queue='long', is_async=True, timeout=54000)

			frappe.msgprint(_('Sales order request created. Wait for the result'))
		
		else:

			frappe.msgprint(_('Import type undefined'))

		return
