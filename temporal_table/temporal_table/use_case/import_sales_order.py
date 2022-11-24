import frappe
from frappe import _

from frappe.utils import now, flt
from frappe.utils.xlsxutils import (
	read_xlsx_file_from_attached_file,
	read_xls_file_from_attached_file,
)

import datetime

from erpnext.controllers.website_list_for_contact import get_customers_suppliers


def import_tso(doc):

	
	v_start_date = now()
	v_error = False
	error_info = ''
	
	
	try:

		load_tmp_sales_order(doc)

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

			if row_item[i+indx] and int(row_item[i+indx]) > 0:

				obj_data = {

					"company": "" if row_item[indx-1] == "None" else row_item[indx-13],
					"customer": "" if row_item[indx-1] == "None" else row_item[indx-12],
					"store": "" if row_item[indx-1] == "None" else row_item[indx-11],
					"product": "" if row_item[indx-1] == "None" else row_item[indx-10],
					"category": "" if row_item[indx-1] == "None" else row_item[indx-9],
					"uom": "" if row_item[indx-1] == "None" else row_item[indx-8],
					"price": "" if row_item[indx-1] == "None" else row_item[indx-7],
					"discount": "" if row_item[indx-1] == "None" else row_item[indx-6],
					"currency": "" if row_item[indx-1] == "None" else row_item[indx-5],
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
			select company, customer, store, product, category, uom, price, discount, currency,
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
			}

			if item.get('store'):

				data_so['warehouse'] = "{} - {}".format(item.get('store'), company_abbr)

			order_items.append(data_so)

		obj_data = {
			"company": so_header.get('company'),
			"customer": item_customer,
			"delivery_date": delivery_date,
			"currency": item_currency,
			"qp_year_week": so_header.get('year_week'),
			"qp_reference1": item.get('reference_1'),
			"qp_reference2": item.get('reference_2'),
			"qp_reference3": item.get('reference_3'),
			"qp_category": so_header.get('category'),
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