# Copyright (c) 2022, Mentum and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe import _

from frappe.utils.password import get_decrypted_password
from frappe.utils import now, make_esc

import os


class qp_Advanced_Integration(Document):

	def validate(self):

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
			frappe.msgprint(_('Please attach file to import or select company'))
			return

		# Validar que haya un tipo de diario selecionado si se va a importar un Journal Entry
		if self.import_type == "qp_je" and not self.journal_type:
			frappe.msgprint(_('Please select Journal Type.'))
			return

		if self.status == 'Active':
			frappe.msgprint(_('Background job already running.'))
			return

		# validar procesos simultánes en la carga de asientos contables
		simultaneous_process = frappe.db.sql("""
				Select count(*)
				from tabqp_Advanced_Integration
				WHERE status = 'Active' and import_type = 'qp_je'""")[0][0] or 0

		if simultaneous_process != 0:
			frappe.msgprint(_('Other background job of Journal Entry already running.'))
			return
		
		self.status = 'Active'
		self.save(ignore_permissions=True)
		self.reload()

		frappe.msgprint(_('Background Job Created. Wait for the result'))

		if self.import_type == "qp_je":

			frappe.enqueue(import_je, doc=self, queue='long', is_async=True, timeout=54000)

		else:

			frappe.msgprint(_('Import type undefined'))

		return


def import_je(doc):
	
	try:

		esc = make_esc('$ ')
		v_start_date = now()
		v_error = False
		error_info = ''

		# TODO: Encontrar como desencriptar el password desde site_config.json
		user_val = frappe.conf.db_name
		pass_val = frappe.conf.db_password
		site_db = frappe.conf.db_name

		# Ubicación del archivo con el resultado, se crea un archivo por documento
		source_file = os.path.join(frappe.get_site_path('private', 'files'), 'result_{0}.txt'.format(doc.name))
		source_file_upd = os.path.join(frappe.get_site_path('private', 'files'), 'result_upd_{0}.txt'.format(doc.name))

		# Se incorpora host y port en la cadena de conexión
		db_port = '-P{0}'.format(frappe.db.port) if frappe.db.port else ''
		db_host = esc(frappe.db.host)

		# TODO: Se asume que es para una empresa a la vez que se realiza el proceso.
		# TODO: Para hacerlo multiempresa se debe Ajustar ESTRUCTURA DEL ARCHIVO PARA A CAMBIAR TRUNCATE POR DELETE.

		source_qry_truncate = "TRUNCATE TABLE  `tabJournal_Entry_Temporal`;"
		count_res, res = execute_simple_query(db_host, db_port, user_val, pass_val, site_db, source_qry_truncate, source_file)

		print("res-->", res)
		print("count_res-->", count_res)

		if not res:
			
			abs_site_path = os.path.abspath(frappe.get_site_path())
			csv_path = ''.join((abs_site_path, doc.import_file))

			# Se sustituye ruta del csv completando con el campo doc.import_file
			source_sql = load_qry_journal_entry_temporal(csv_path)
			var_sql = "mysql -h {0} {1} -u {2} -p{3} {4} > {5} {6}".format(
				db_host, db_port, user_val, pass_val, site_db, source_file, source_sql)
			print("var_sql-->", var_sql)
			os.system(var_sql)
			count_res, res = read_result(source_file)

			print("res-->", res)
			print("count_res-->", count_res)

			# Validar el registro de la tabla tabJournal_Entry_Temporal
			source_qry_voucher = """<<EOF
Select count(*)
from tabJournal_Entry_Temporal;
EOF"""
			var_sql = "mysql -h {0} {1} -u {2} -p{3} {4} > {5} {6}".format(
				db_host, db_port, user_val, pass_val, site_db, source_file_upd, source_qry_voucher)
			print("var_sql-->", var_sql)
			os.system(var_sql)
			res_upd, data = read_result_detail(source_file_upd)

			print("res-->", res)
			print("res_upd-->", res_upd)

			if not res and res_upd:

				# Se paramertiza ubicación del archivo con el qry
				source_procedure = os.path.join(abs_site_path, os.path.dirname(__file__), 'import_journal_entry.sql')

				var_sql = "mysql -h {0} {1} -u {2} -p{3} {4} < {5} > {6}".format(
					db_host, db_port, user_val, pass_val, site_db, source_procedure, source_file)
				print("var_sql-->", var_sql)
				os.system(var_sql)
				sp_result, res = read_result_detail(source_file, sp=True)

				v_error = not sp_result

			else:

				error_info = res if res else "File upload failed. Check the format or content used for the upload."
				v_error = True
		
		else:

			v_error = True

	except Exception as error:

		v_error = True

		frappe.log_error(message=frappe.get_traceback(), title="import_je")

		pass

	try:

		res_msg = res if not error_info else error_info

		doc.append('import_list', {
				"status": "Failed" if v_error else "Completed",
				"start_date": v_start_date,
				"result_message": res_msg or "View ERP Log or worker Log",
				"finish_date": now()
			})
		doc.status = "Failed" if v_error else "Completed"
		doc.save(ignore_permissions=True)

	except Exception as error:

		frappe.log_error(message=frappe.get_traceback(), title="import_je_result")

		pass

	return

def execute_simple_query(db_host, db_port, user_val, pass_val, site_db, source_qry, source_file, sql_detail = False):
	
	var_sql = "mysql -h {0} {1} -u {2} -p{3} {4} -e '{5}' > {6}".format(
		db_host, db_port, user_val, pass_val, site_db, source_qry, source_file)
	print("var_sql-->", var_sql)
	os.system(var_sql)
	count_res, res = read_result_detail(source_file) if sql_detail else read_result(source_file)

	return count_res, res




def load_qry_journal_entry_temporal(je_file_path):

	qry_load_journal_entry_temporal = """<<EOF
LOAD DATA LOCAL INFILE '{0}'
INTO TABLE tabJournal_Entry_Temporal
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES
(id,entry_type,series,company,posting_date,title,total_debit,total_credit,name,party_type,party,account,debit_in_account_currency,credit_in_account_currency);
EOF""".format(je_file_path)

	return qry_load_journal_entry_temporal


def read_result(res_file):

	print("-----read_result-----")

	data = ""

	try:

		with open(res_file, "r") as source_file:
			data = str(source_file.read())

	except Exception as error:

		data = frappe.get_traceback()

		pass

	return False, data


def read_result_detail(res_file, sp=False):

	print("-----read_result_detail----- sp", sp)

	result = False

	data = ""

	try:

		with open(res_file, "r") as source_file:
			data = str(source_file.read())
			lines = data.splitlines()
			if sp:
				print("data", data)
				result = True if len(lines) == 2 and lines[0] == 'result' and int(lines[1]) == 1 else False
			else:
				print("lines --->", lines)
				result = True if len(lines) == 2 and lines[0] == 'count(*)' and int(lines[1]) > 0 else False
				
	except Exception as error:

		frappe.log_error(message=frappe.get_traceback(), title="read_result_detail")

		pass

	return result, data
