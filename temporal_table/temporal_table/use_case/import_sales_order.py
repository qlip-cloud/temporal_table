import frappe
from frappe import _
import datetime

from frappe.utils import now, flt, getdate
from frappe.utils.xlsxutils import (
	read_xlsx_file_from_attached_file,
	read_xls_file_from_attached_file,
)

import datetime

from erpnext.controllers.website_list_for_contact import get_customers_suppliers


def import_tso(doc):

	print("import_tso----------->", now)

	
	v_start_date = now()
	v_error = False
	error_info = ''
	
	
	try:

		load_tmp_sales_order(doc)

		load_sales_order(doc) 

	except Exception as error:

		frappe.db.rollback()

		v_error = True

		frappe.log_error(message= str(error), title="import_tso:{}".format(doc.name))
		# frappe.log_error(message=frappe.get_traceback(), title="import_tso:{}".format(doc.name))

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

	indx = 15
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

			if row_item[3] and row_item[i+indx] and int(row_item[i+indx]) > 0:
				# se salta descripción del producto (indx - 10)
				obj_data = {
					"company": doc.company if row_item[indx-15] == "None" or row_item[indx-15] == None else row_item[indx-15],
					"customer": None if row_item[indx-14] == "None" or row_item[indx-14] == None else row_item[indx-14],
					"store": None if row_item[indx-13] == "None" or row_item[indx-13] == None else row_item[indx-13],
					"product": None if row_item[indx-12] == "None" or row_item[indx-12] == None else row_item[indx-12],
					"item_type": None if row_item[indx-11] == "None" or row_item[indx-11] == None else row_item[indx-11],
					"category": None if row_item[indx-9] == "None" or row_item[indx-9] == None else row_item[indx-9],
					"uom": None if row_item[indx-8] == "None" or row_item[indx-8] == None else row_item[indx-8],
					"price": None if row_item[indx-7] == "None" or row_item[indx-7] == None else row_item[indx-7],
					"empty_price": 1 if row_item[indx-7] == "None" or row_item[indx-7] == None else 0,
					"discount": None if row_item[indx-6] == "None" or row_item[indx-6] == None else row_item[indx-6],
					"currency": None if row_item[indx-5] == "None" or row_item[indx-5] == None else row_item[indx-5],
					"shipping_address": None if row_item[indx-4] == "None" or row_item[indx-4] == None else row_item[indx-4],
					"reference_1": None if row_item[indx-3] == "None" or row_item[indx-3] == None else row_item[indx-3],
					"reference_2": "" if row_item[indx-2] == "None" or row_item[indx-2] == None else row_item[indx-2],
					"reference_3": "" if row_item[indx-1] == "None" or row_item[indx-1] == None else row_item[indx-1],
					"year_week": row_header[i],
					"product_qty": row_item[i+indx],
					"origin_process": doc.name,
					"doctype": "qp_tmp_sales_orders"
				}

				temp_sale_order =  frappe.get_doc(obj_data)

				temp_sale_order.insert(ignore_permissions=True)


def load_sales_order(doc):

	validation_result, validation_msg = validate_so2save(doc.name, doc.company)

	if validation_result:

		raise Exception("Validation of required fields: {}".format(validation_msg))

	# Se asume que es un archivo por cliente
	# Si no hay registrado un cliente se toma el cliente seleccionado por el usuario tomado del doc advanced
	item_customer = __get_item_customer(doc.name, doc.customer, doc.company)

	# Se agrega validación para garantizar que hay un único SO para cada cabecera
	if is_duplicated(doc.name, item_customer):

		raise Exception("Sales Order duplicated in document: {}".format(doc.name))

	data = get_headers(doc.name)

	# Se agrega validación porque no se permite cambiar la moneda a ordenes de venta ya creadas
	# NOTA: Al permitir para la misma semana diferentes monedas, no hay manera de detectar cual es la orden a comparar
	# res_cur, message_cur = is_other_currency(doc.name, item_customer)
	# if res_cur:

	# 	raise Exception("Sales Order with other currency: {}".format(message_cur))

	for so_header in data:

		delivery_date = __transform_year_week(so_header.get('year_week'))

		item_data = get_details(doc.name, so_header)

		order_items = []

		prod_type = ""

		item_currency = ""

		item_shipping_address = ""

		list_price_header = ""

		for item in item_data:

			if not item_currency:

				item_currency = item.get("currency") or ""
			
			if not item_shipping_address:

				item_shipping_address = item.get("shipping_address") or ""

			if not list_price_header:

				list_price_header = get_list_price(so_header.get('company'), item.get('product'))

			prod_id = item.get('product')

			prod_type = item.get('item_type') or ""

			prod_name = frappe.db.get_value("Item", prod_id, "item_name")

			prod_uom = frappe.db.get_value("Item", prod_id, "stock_uom")
			prod_uom = item.get('uom') or prod_uom

			uom_from_list = __get_uom_from_list(prod_id, prod_uom)
			item_uom_conv = uom_from_list and uom_from_list[0]['conversion_factor'] or 1

			company_abbr = frappe.db.get_value("Company", so_header.get('company'), "abbr")

			item_disc = item.get('discount') or 0

			# Tomar el precio de la lista de precio por defecto del producto en caso de no existir en el archivo
			# Detectar cuando está en blanco y cuando el precio es cero
			if str(item.get('empty_price')) == "0":
				item_amount = item.get('price')
			else:
				price_list = get_list_price(so_header.get('company'), item.get('product'))
				price_list = price_list or list_price_header
				price_list = price_list or frappe.db.get_single_value("Selling Settings", "selling_price_list")

				price_default = frappe.get_list('Item Price', filters={'item_code': prod_id, 'price_list': price_list}, fields=['price_list_rate'])
				price_default = price_default and price_default[0]['price_list_rate'] or 0
				item_amount = price_default

			data_so = {
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
				"qp_item_type": prod_type
			}

			if item.get('store'):

				data_so['warehouse'] = "{} - {}".format(item.get('store'), company_abbr)

			order_items.append(data_so)

		# Verificar si existe para actualizar en lugar de insertar
		rec_so = search_sales_order(item_customer, so_header)

		if rec_so:
			#Update

			so_obj = frappe.get_doc('Sales Order', rec_so[0].name)

			so_obj.items = []

			for item_so in order_items:
				so_obj.append("items", item_so)

			if item_shipping_address:

				so_obj.shipping_address_name = item_shipping_address

			# Guardar historial de cambios del proceso
			historial_obj = prepare_process_history(so_obj.qp_origin_process)

			so_obj.append('process_history', historial_obj)

			so_obj.qp_origin_process = doc.name

			if so_obj.qp_gp_status == 'sent':

				so_obj.qp_gp_status = 'updated'

			so_obj.is_updated = '1'

			so_obj.save(ignore_permissions=True)

		else:
			# Insert

			obj_data = {
				"company": so_header.get('company'),
				"customer": item_customer,
				"delivery_date": delivery_date,
				"qp_year_week": so_header.get('year_week'),
				"qp_reference1": so_header.get('reference_1'),
				"qp_reference2": so_header.get('reference_2'),
				"qp_reference3": item.get('reference_3'),
				"qp_category": so_header.get('category'),
				"qp_origin_process": doc.name,
				"is_updated": '0',
				"items": order_items,
				"doctype": "Sales Order"
			}

			if item_currency:

				obj_data['currency'] = item_currency

			if item_shipping_address:

				obj_data['shipping_address_name'] = item_shipping_address

			if list_price_header:

				obj_data['selling_list_price'] = list_price_header

			sale_order = frappe.get_doc(obj_data)

			sale_order.insert(ignore_permissions=True)


def validate_so2save(doc_name, doc_company):

	msg_res = ""

	# Validar campos

	if __multiple_companies(doc_name, doc_company):

		msg_res += _("There are multiple companies in the file and/or it does not correspond to the selected company<br>\n")

	if __products_belong_to_company(doc_name):

		msg_res += _("There are products that do not belong to the company<br>\n")

	if __store_belong_to_company(doc_name, doc_company):

		msg_res += _("There are stores that do not belong to the company<br>\n")

	#if __group_by_currency(doc_name):

		#msg_res += _("There is different currency for a document or there is no currency<br>\n")

	if __group_by_shipping_address(doc_name):

		msg_res += _("There is different shipping address for a document or there is no shipping address<br>\n")

	# Validar year_week
	if __get_invalid_week_number(doc_name):

		msg_res += _("There is an invalid week number<br>\n")


	# Validar productos duplicados (distinguiendo por item_type)
	res, prod_list = __duplicate_products(doc_name)
	if res:

		msg_res += _("There is duplicate products: {} <br>\n".format(str(prod_list)))

	return msg_res and True or False, msg_res


def is_duplicated(doc_name, item_customer):

	sql_str = """
		select count(name) from
		(select company, category, reference_1, year_week, currency, reference_2
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}'
		group by  company, category, reference_1, year_week, currency, reference_2) as temp
		inner join `tabSales Order` as so on so.company = temp.company and so.qp_category = temp.category
		and so.qp_reference1 = temp.reference_1 and so.qp_year_week = temp.year_week  and so.currency = temp.currency
		and so.qp_reference2 = temp.reference_2
		Where so.customer = '{customer}'
		group by so.company, so.customer, so.qp_category, so.qp_reference1, so.qp_year_week, so.currency, so.qp_reference2
		having count(name) > 1
	""".format(origin_process=doc_name, customer=item_customer)
	data = frappe.db.sql(sql_str, as_dict=1)

	return data and True or False


def get_headers(doc_name):

	sql_str = """
		select company, category, reference_1, year_week, currency, reference_2
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}'
		group by company, category, reference_1, year_week, currency, reference_2
		order by company, category, reference_1, year_week, currency, reference_2
	""".format(origin_process=doc_name)

	data = frappe.db.sql(sql_str, as_dict=1)

	return data


def get_details(doc_name, so_header):

	sql_str = """
		select company, customer, store, product, item_type, category, uom, price, empty_price, discount, currency,
		shipping_address, reference_1, reference_2, reference_3, year_week, product_qty
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}' and company = '{company}'
		and category = '{category}' and reference_1 = '{reference_1}' and year_week = '{year_week}'
		and currency = '{currency}' and reference_2 = '{reference_2}'
		order by company, category, reference_1, year_week, product, currency, reference_2
	""".format(origin_process=doc_name, company=so_header.get('company'), category=so_header.get('category'),
		reference_1=so_header.get('reference_1'), year_week=so_header.get('year_week'),
		currency=so_header.get('currency'), reference_2=so_header.get('reference_2'))

	item_data = frappe.db.sql(sql_str, as_dict=1)

	return item_data


def search_sales_order(item_customer, so_header):

	so_sql = """
		select name
		from `tabSales Order`
		where company = '{company}' and customer = '{customer}'
		and qp_category = '{category}'
		and currency = '{currency}'
		and qp_reference1 = '{reference_1}' and qp_year_week = '{year_week}' and qp_reference2 = '{reference_2}'
	""".format(company=so_header.get('company'), customer=item_customer,
		category=so_header.get('category'),
		reference_1=so_header.get('reference_1'), year_week=so_header.get('year_week'),
		currency = so_header.get('currency'), reference_2=so_header.get('reference_2'))

	rec_so = frappe.db.sql(so_sql, as_dict=1)

	return rec_so


def prepare_process_history(doc_process):

	detail = {
		"date": now(),
		"origin_process": doc_process
	}

	return detail


def __get_invalid_week_number(doc_name):

	now_week_number = datetime.datetime.now().isocalendar()

	week_number = "{0}-{1}".format(now_week_number[0], str(now_week_number[1]).rjust(2, '0'))

	day_week_number	= datetime.date.fromisocalendar(now_week_number[0], now_week_number[1], now_week_number[2]).day

	curr_now_week_number = str(now_week_number[0]) + '-' + str(now_week_number[1]).rjust(2, '0') + '-' + str(day_week_number).rjust(2, '0')

	# Validar que year_week esté vigente (se verifica los dos formatos que se manejan: 'yyyy-ww' y 'yyyy-ww-dd')
	sql_str = """
		Select year_week
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}' and LENGTH(year_week) = 7 and year_week <= '{now_week_number}'
		UNION ALL
		Select year_week
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}' and LENGTH(year_week) = 10 and year_week < '{curr_now_week_number}'
	""".format(origin_process=doc_name, now_week_number=week_number, curr_now_week_number=curr_now_week_number)
	res = frappe.db.sql(sql_str, as_dict=1)

	return res and True or False


def __duplicate_products(doc_name):

	sql_str = """
		select company, category, reference_1, year_week, product, item_type, currency, reference_2, count(product) as count_prod
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}'
		group by company, category, reference_1, year_week, product, item_type, currency, reference_2
		having count_prod > 1
	""".format(origin_process=doc_name)

	data = frappe.db.sql(sql_str, as_dict=1)

	return data and True or False, [x.product for x in data]


def __multiple_companies(doc_name, doc_company):

	# Debe haber una compañía en el grupo a guardar y corresponder con el del archivo

	result = True

	sql_str = """
		select distinct company
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}'
	""".format(origin_process=doc_name)
	res = frappe.db.sql(sql_str, as_dict=1)

	if len(res) == 1 and doc_company == res[0].company:

		result = False

	return result


def __products_belong_to_company(doc_name):

	# Los productos deben pertenecer a la compañía

	result = True

	sql_str = """
		select count(product) as prodt_tot
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}'
		group by product
	""".format(origin_process=doc_name)
	res_prod = frappe.db.sql(sql_str, as_dict=1)

	sql_str = """
		select count(tmp_so.product) as prodt_tot
		from tabqp_tmp_sales_orders tmp_so
		inner join `tabItem Default` item_def
		on tmp_so.product = item_def.parent and tmp_so.company = item_def.company
		and item_def.parentfield = 'item_defaults'
		and item_def.parenttype = 'Item'
		where origin_process = '{origin_process}'
		group by tmp_so.product
	""".format(origin_process=doc_name)
	res = frappe.db.sql(sql_str, as_dict=1)

	if res_prod and res and res_prod[0]['prodt_tot'] == res[0]['prodt_tot']:

		result = False

	return result

def __store_belong_to_company(doc_name, doc_company):

	# Las bodegas deben pertenecer a la compañía

	sql_str = """

		select drb_tmp.store from
		(select distinct store
		from tabqp_tmp_sales_orders tmp_so
		where origin_process = '{origin_process}') as drb_tmp
		where drb_tmp.store not in (
			select SUBSTRING_INDEX(name, ' - ', 1) as store
			from `tabWarehouse`
			where company = '{company_id}')
	""".format(origin_process=doc_name, company_id = doc_company)
	res = frappe.db.sql(sql_str, as_dict=1)

	return res and True or False


def __group_by_currency(doc_name):

	# Validar que sea un mismo tipo de moneda por sales order a crear
	sql_str = """
		Select count(currency) as curr from (
			select company, category, reference_1, year_week, reference_2, currency
			from tabqp_tmp_sales_orders
			where origin_process = '{origin_process}'
			group by  company, category, reference_1, year_week, reference_2, currency
		) as dbtbl
		group by company, category, reference_1, year_week, reference_2 
		having curr > 1
	""".format(origin_process=doc_name)
	res = frappe.db.sql(sql_str, as_dict=1)

	return res and True or False


def __group_by_shipping_address(doc_name):

	# Validar que sea un mismo tipo de dirección de envío por sales order a crear
	sql_str = """
		Select count(shipping_address) as ship_addr from (
			select company, category, reference_1, year_week, shipping_address, currency, reference_2
			from tabqp_tmp_sales_orders
			where origin_process = '{origin_process}'
			group by  company, category, reference_1, year_week, shipping_address, currency, reference_2
		) as dbtbl
		group by company, category, reference_1, year_week, currency, reference_2
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
			group by  tso.company, tso.category, tso.reference_1, tso.year_week, tso.shipping_address, tso.currency, tso.reference_2
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

	res = getdate()

	date_content = year_week.split('-')

	if len(date_content) == 2:

		param_year = "{0}-W{1}".format(date_content[0], date_content[1])

		# primer lunes de la semana
		res = datetime.datetime.strptime(param_year + '-1', "%Y-W%W-%w")

	return res


def __get_item_customer(origin_process, param_customer, param_company):

	res = ""

	sql_str = """
		select distinct customer
		from tabqp_tmp_sales_orders
		where origin_process = '{origin_process}'
	""".format(origin_process=origin_process)
	data = frappe.db.sql(sql_str, as_dict=1)

	if (len(data) == 1 and data[0].customer and data[0].customer == param_customer) or (len(data) == 1 and not data[0].customer):

		res = param_customer

	else:

		raise Exception("File client mismatch: {} Result: {}".format(param_customer, data))

	# Validar que el cliente a registrar pertenece a la compañía

	sql_str = """
		select parent as customer_id
		from `tabParty Account`
		where parent = '{customer_id}' and company = '{company_id}' and parentfield = 'accounts' and parenttype = 'Customer'
	""".format(customer_id=res, company_id=param_company)
	res_customer = frappe.db.sql(sql_str, as_dict=1)

	if not res_customer:

		raise Exception("The client does not belong to the company")

	return res


def is_other_currency(doc_name, item_customer):

	so_sql = """
		SELECT drb_so.name FROM
			(select company, category, reference_1, year_week, currency, reference_2
			from tabqp_tmp_sales_orders
			where origin_process = '{origin_process}'
			group by  company, category, reference_1, year_week, currency, reference_2) as drb_temp
		INNER JOIN
			(select name, company, qp_category, qp_reference1, qp_year_week, currency, qp_reference2
			from `tabSales Order`
			where customer = '{customer}') as drb_so
			ON drb_temp.company = drb_so.company and drb_temp.category = drb_so.qp_category
			and drb_temp.reference_1 = drb_so.qp_reference1
			and drb_temp.year_week = drb_so.qp_year_week
			and drb_temp.reference_2 = drb_so.qp_reference2
		WHERE drb_temp.currency != drb_so.currency
	""".format(origin_process=doc_name, customer=item_customer)

	rec_so = frappe.db.sql_list(so_sql)

	return rec_so and True or False, str(rec_so)


def get_list_price(company, product_id):

	so_sql = """
		SELECT drb_def.default_price_list FROM `tabItem Default` as drb_def
		INNER JOIN tabItem as drb_item on drb_def.parent =  drb_item.name
		WHERE drb_def.parentfield = 'item_defaults' AND drb_def.parenttype = 'Item'
		AND drb_def.company = '{company_name}' AND drb_def.parent = '{product}'
		LIMIT 1;
	""".format(company_name=company, product=product_id)

	rec_price_list = frappe.db.sql_list(so_sql)

	return rec_price_list and rec_price_list[0] or ''
