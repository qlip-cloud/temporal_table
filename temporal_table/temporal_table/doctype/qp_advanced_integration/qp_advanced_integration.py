# Copyright (c) 2022, Mentum and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe import _

from frappe.utils.password import get_decrypted_password
from frappe.utils import now, make_esc, flt
from frappe.utils.xlsxutils import (
	read_xlsx_file_from_attached_file,
	read_xls_file_from_attached_file,
)

import os

import datetime

from erpnext.controllers.website_list_for_contact import get_customers_suppliers


class qp_Advanced_Integration(Document):

	def validate(self):

		root, extension = os.path.splitext(self.import_file)
		if extension != ".csv" and self.import_type == "qp_je":
			frappe.throw(_("Allowed extension .csv"))
		
		if extension not in (".xlsx", ".xls") and self.import_type == "qp_tso":
			frappe.throw(_("Allowed extension .xlsx"))


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

		if self.import_type == "qp_tso":

			frappe.enqueue(import_tso, doc=self, queue='long', is_async=True, timeout=54000)
		
		else:

			frappe.msgprint(_('Import type undefined'))

		return


def import_je(doc):
	
	try:

		esc = make_esc('$ ')
		v_start_date = now()
		v_error = False
		error_info = ''

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
				result = True if len(lines) == 6 and lines[0] == 'total_lines_processed' and int(lines[1]) > 0 and lines[2] == 'document' and lines[3] != ''  and lines[4] == 'result' and int(lines[5]) == 1 else False
			else:
				print("lines --->", lines)
				result = True if len(lines) == 2 and lines[0] == 'count(*)' and int(lines[1]) > 0 else False
				
	except Exception as error:

		frappe.log_error(message=frappe.get_traceback(), title="read_result_detail")

		pass

	return result, data


def import_tso(doc):

	
	v_start_date = now()
	v_error = False
	error_info = ''
	
	
	try:

		res = load_tmp_sales_order(doc)

		load_sales_order(doc) 

	except Exception as error:

		frappe.db.rollback()

		v_error = True

		frappe.log_error(message=frappe.get_traceback(), title="import_tso")

		pass

	try:

		doc.append('import_list', {
				"status": "Failed" if v_error else "Completed",
				"start_date": v_start_date,
				"result_message": "View ERP Log or worker Log" if v_error else "Successful",
				"finish_date": now()
			})
		doc.status = "Failed" if v_error else "Completed"
		doc.save(ignore_permissions=True)

	except Exception as error:

		frappe.log_error(message=frappe.get_traceback(), title="import_tso_result")

		pass

	return


def load_tmp_sales_order(doc):

	indx = 13
	data = []

	frappe.db.sql("delete from `tabqp_tmp_sales_orders` where origin_process = '{0}'".format(doc.name))

	up_file = frappe.get_doc("File", {"file_url": doc.import_file})
	parts = up_file.get_extension()
	extension = parts[1]
	content = up_file.get_content()
	extension = extension.lstrip(".")

	if extension == "xlsx":
		data = read_xlsx_file_from_attached_file(fcontent=content)
	elif extension == "xls":
		data = read_xls_file_from_attached_file(content)

	row_header = data[0][indx:]
	content_list = data[1:]

	for i in range(len(row_header)):
		for row_item in content_list:

			obj_data = {

				"company": "" if row_item[indx-1] == "None" else row_item[indx-13],
				"customer": "" if row_item[indx-1] == "None" else row_item[indx-12],
				"store": "" if row_item[indx-1] == "None" else row_item[indx-11],
				"product": "" if row_item[indx-1] == "None" else row_item[indx-10],
				"category": "" if row_item[indx-1] == "None" else row_item[indx-9],
				"price": "" if row_item[indx-1] == "None" else row_item[indx-8],
				"discount": "" if row_item[indx-1] == "None" else row_item[indx-7],
				"currency": "" if row_item[indx-1] == "None" else row_item[indx-6],
				"uom": "" if row_item[indx-1] == "None" else row_item[indx-5],
				"shipping_address": "" if row_item[indx-1] == "None" else row_item[indx-4],
				"reference_1": "" if row_item[indx-1] == "None" else row_item[indx-3],
				"reference_2": "" if row_item[indx-1] == "None" else row_item[indx-2],
				"reference_3": "" if row_item[indx-1] == "None" else row_item[indx-1],
				"year_week": row_header[i],
				"product_qty": row_item[i+indx],
				"origin_process": doc.name,
				"doctype": "qp_tmp_sales_orders"
			}

			temp_sale_order =  frappe.get_doc(obj_data)

			temp_sale_order.insert(ignore_permissions=True)


def load_sales_order(doc):

	validation_result, validation_msg = validate_so2save(doc.name)

	if validation_result:

		raise Exception("Validation of required fields: {}".format(validation_msg))

	sql_str = """
		select company, category, reference_1, year_week
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}'
		group by  company, category, reference_1, year_week
		order by company, category, reference_1, year_week
	""".format(origin_process=doc.name)

	data = frappe.db.sql(sql_str, as_dict=1)

	for so_header in data:

		delivery_date = __transform_year_week(so_header.get('year_week'))

		sql_str = """
			select company, customer, store, product, category, price, discount, currency, uom, 
			shipping_address, reference_1, reference_2, reference_3, year_week, product_qty
			from tabqp_tmp_sales_orders
			where origin_process = '{origin_process}' and company = '{company}'
			and category = '{category}' and reference_1 = '{reference_1}' and year_week = '{year_week}'
			order by company, category, reference_1, year_week, product
		""".format(origin_process=doc.name, company=so_header.get('company'), category=so_header.get('category'),
			reference_1=so_header.get('reference_1'), year_week=so_header.get('year_week'))

		item_data = frappe.db.sql(sql_str, as_dict=1)

		order_items = []

		item_customer = ""

		item_currency = ""

		item_shipping_address = ""

		for item in item_data:

			# Se asume que es un archivo por cliente
			if not item_customer:

				item_customer = __get_customer_name(item.get('customer') or "")

			if not item_currency:

				item_currency = item.get("currency") or ""
			
			if not item_shipping_address:

				item_shipping_address = item.get("shipping_address") or ""

			prod_id = item.get('product')

			prod_name = frappe.db.get_value("Item", prod_id, "item_name")

			prod_uom = frappe.db.get_value("Item", prod_id, "stock_uom")
			prod_uom = item.get('uom') or prod_uom

			uom_from_list = __get_uom_from_list(prod_id, prod_uom)
			item_uom_conv = uom_from_list and uom_from_list[0]['conversion_factor'] or 1

			company_abbr = frappe.db.get_value("Company", so_header.get('company'), "abbr")

			item_disc = item.get('discount') or 0

			# TODO: Definir de donde tomar el precio en caso de no existir en el archivo
			# La solución aplicada asume que se antiene el proceso de actualización de precios de VF
			price_list = frappe.db.get_single_value("Selling Settings", "selling_price_list")
			price_default = frappe.get_list('Item Price', filters={'item_code': prod_id, 'price_list': price_list}, fields=['price_list_rate'])
			price_default = price_default and price_default[0]['price_list_rate'] or 0
			item_amount = item.get('price') or price_default

			# TODO: Validar cantidad > 0 

			order_items.append({
				"item_code": prod_id,
				"item_name": prod_name,
				"description": 'PROD: {0} - CAT: {1}'.format(prod_id, so_header.get('category')),
				"rate": flt(item_amount) - (flt(item_amount) * flt(item_disc)/100),
				"qty": item.get('product_qty'),
				"stock_uom": prod_uom,
				"price_list_rate": item_amount,
				"margin_type": 'Percentage',
				"discount_percentage": item.get('discount'),
				"discount_amount": flt(item_amount) * flt(item_disc)/100,
				"conversion_factor": item_uom_conv,
				"delivery_date": delivery_date,
				"warehouse": "{} - {}".format(item.get('store'), company_abbr),
			})

		obj_data = {
			"company": so_header.get('company'),
			"customer": item_customer,
			"delivery_date": delivery_date,
			"currency": item_currency,
			"qp_year_week": so_header.get('year_week'),
			"qp_reference1": item.get('reference_1'),
			"qp_reference2": item.get('reference_2'),
			"qp_reference3": item.get('reference_3'),
			"qp_origin_process": doc.name,
			"items": order_items,
			"doctype": "Sales Order"
		}

		if item_shipping_address:

			obj_data['shipping_address_name'] = item_shipping_address

		# print("obj_data--->", obj_data)

		sale_order = frappe.get_doc(obj_data)

		sale_order.insert(ignore_permissions=True)


def validate_so2save(doc_name):

	msg_res = ""

	# Validar campos

	if __group_by_currency(doc_name):

		msg_res += _("There is different currency for a document or there is no currency\n")

	if __group_by_shipping_address(doc_name):

		msg_res += _("There is different shipping address for a document or there is no shipping address\n")

	return msg_res and True or False, msg_res


def __group_by_currency(doc_name):

	# Validar que sea un mismo tipo de moneda por sales order a crear
	sql_str = """
		Select count(currency) as curr from (
			select company, category, reference_1, year_week, currency
			from tabqp_tmp_sales_orders
			where origin_process = '{origin_process}'
			group by  company, category, reference_1, year_week, currency 
		) as dbtbl
		group by company, category, reference_1, year_week
		having curr > 1
	""".format(origin_process=doc_name)
	res = frappe.db.sql(sql_str, as_dict=1)

	return res and True or False


def __group_by_shipping_address(doc_name):

	# Validar que sea un mismo tipo de dirección de envío por sales order a crear
	sql_str = """
		Select count(shipping_address) as ship_addr from (
			select company, category, reference_1, year_week, shipping_address
			from tabqp_tmp_sales_orders
			where origin_process = '{origin_process}'
			group by  company, category, reference_1, year_week, shipping_address 
		) as dbtbl
		group by company, category, reference_1, year_week
		having ship_addr > 1
	""".format(origin_process=doc_name)
	res = frappe.db.sql(sql_str, as_dict=1)

	result = res and True or False

	# Validar que exista la dirección de envío en la BD
	if not result:

		sql_str = """
			select tso.shipping_address, addr.name
			from tabqp_tmp_sales_orders as tso
			left outer join tabAddress as addr on tso.shipping_address = addr.name
			where tso.origin_process = '{origin_process}' and addr.name is Null and tso.shipping_address is not Null
			group by  tso.company, tso.category, tso.reference_1, tso.year_week, tso.shipping_address
		""".format(origin_process=doc_name)
		res = frappe.db.sql(sql_str, as_dict=1)

		result = res and True or False

	return result


def __get_uom_from_list(item_name, item_uom):

	return frappe.db.get_list('UOM Conversion Detail',
		filters={
			'parenttype': 'Item',
			'parentfield': 'uoms',
			'parent': item_name,
			'uom': item_uom
		},
		fields=['uom', 'conversion_factor'],
		order_by='conversion_factor desc',
	)


def __transform_year_week(year_week):

	date_content = year_week.split('-')

	param_year = "{0}-W{1}".format(date_content[0], date_content[1])

	# primer lunes de la semana
	return datetime.datetime.strptime(param_year + '-1', "%Y-W%W-%w")


def __get_customer_name(so_header_customer):

	if so_header_customer:

		item_customer = so_header_customer

	else:

		customer = __get_customer()
		item_customer = customer.name
	
	return item_customer


def __get_customer():

    user = frappe.session.user

    # find party for this contact
    customers, suppliers = get_customers_suppliers('Sales Order', user)

    if len(customers) < 1:

        frappe.throw(_("User does not have an associated client"))

    customer = frappe.get_doc('Customer', customers[0])

    return customer