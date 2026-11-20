import frappe
from frappe import _

from frappe.utils import now

import os
import re
import subprocess


# ---------------------------------------------------------------------------
# Hardening (CWE-78 / CWE-89 / CWE-22 / CWE-532)
# - Nunca se invoca un shell: subprocess con lista de argumentos (shell=False).
# - El SQL se entrega por stdin (input=) en vez de heredoc <<EOF, que el shell
#   expande ($(...), `...`, $VAR).
# - La contraseña viaja por la variable de entorno MYSQL_PWD del proceso hijo,
#   no por la línea de comandos (visible en `ps`) ni en logs.
# - La ruta del CSV se valida con allowlist y se confina a <site>/private/files.
# ---------------------------------------------------------------------------

MYSQL_BIN = "mysql"
MYSQL_TIMEOUT = 54000  # igual al timeout del job encolado

# Caracteres prohibidos en la ruta del CSV: rompen el literal SQL de
# LOAD DATA o son caracteres de control.
_FORBIDDEN_PATH_CHARS = re.compile(r"[\'\"\\`\x00-\x1f\x7f]")
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_\-]")


def _safe_doc_name(name):
	"""Nombre seguro para usar en nombres de archivo (evita path traversal)."""
	return _SAFE_NAME.sub("_", str(name or ""))[:140] or "unnamed"


def _build_mysql_cmd(db_host, db_port, user_val, site_db, extra_args=None):
	"""Construye argv para mysql. Sin shell, cada valor es un argumento atómico."""
	cmd = [MYSQL_BIN, "-h", str(db_host), "-u", str(user_val)]
	if db_port:
		# db_port llega como '-P<puerto>' (compatibilidad con la firma original)
		port = str(db_port)
		port = port[2:] if port.startswith("-P") else port
		if not port.isdigit():
			raise ValueError("Invalid DB port")
		cmd += ["-P", port]
	if extra_args:
		cmd += list(extra_args)
	cmd.append(str(site_db))
	return cmd


def _run_mysql(cmd, pass_val, output_file, stdin_text=None, stdin_file=None):
	"""Ejecuta mysql sin shell, escribiendo stdout en output_file (equivale a '> file')."""
	env = dict(os.environ)
	env["MYSQL_PWD"] = pass_val or ""

	with open(output_file, "w") as out:
		if stdin_file:
			with open(stdin_file, "r") as fin:
				proc = subprocess.run(cmd, stdin=fin, stdout=out, stderr=subprocess.PIPE,
					env=env, shell=False, timeout=MYSQL_TIMEOUT)
		else:
			proc = subprocess.run(cmd, input=(stdin_text or "").encode("utf-8"),
				stdout=out, stderr=subprocess.PIPE, env=env, shell=False, timeout=MYSQL_TIMEOUT)

	if proc.returncode != 0:
		# stderr de mysql no incluye la contraseña (no está en argv)
		frappe.log_error(message=proc.stderr.decode("utf-8", "replace")[:4000], title="import_je mysql")

	return proc.returncode


def _resolve_import_file(abs_site_path, import_file):
	"""Valida doc.import_file y devuelve la ruta absoluta del CSV dentro de private/files."""
	if not import_file or not isinstance(import_file, str):
		frappe.throw(_("Invalid import file"))

	if _FORBIDDEN_PATH_CHARS.search(import_file):
		frappe.throw(_("Invalid characters in import file path"))

	if not import_file.startswith("/private/files/") or os.path.splitext(import_file)[1].lower() != ".csv":
		frappe.throw(_("Import file must be a private .csv file"))

	# Debe corresponder a un File real registrado en el sistema
	if not frappe.db.exists("File", {"file_url": import_file}):
		frappe.throw(_("Import file not found"))

	allowed_dir = os.path.realpath(os.path.join(abs_site_path, "private", "files"))
	csv_path = os.path.realpath(''.join((abs_site_path, import_file)))

	if os.path.commonpath([allowed_dir, csv_path]) != allowed_dir or not os.path.isfile(csv_path):
		frappe.throw(_("Import file outside allowed directory"))

	# Revalidar tras resolver symlinks
	if _FORBIDDEN_PATH_CHARS.search(csv_path):
		frappe.throw(_("Invalid characters in import file path"))

	return csv_path


def _is_import_je_enabled():
	"""
	Interruptor de la funcionalidad. Desactivada por defecto.
	Para activarla sin tocar código, en site_config.json:
	    "enable_import_journal_entry": 1
	(bench --site <sitio> set-config enable_import_journal_entry 1)
	"""
	return bool(frappe.conf.get("enable_import_journal_entry"))


def import_je(doc):

	# --- FUNCIONALIDAD DESACTIVADA TEMPORALMENTE ---
	# Se cierra el documento como Failed para que no quede en 'Active'
	# (el controlador lo marca Active antes de encolar y eso bloquearía otras cargas).
	if not _is_import_je_enabled():
		try:
			doc.append('import_list', {
				"status": "Failed",
				"start_date": now(),
				"result_message": "Journal Entry import is temporarily disabled.",
				"finish_date": now()
			})
			doc.status = "Failed"
			doc.save(ignore_permissions=True)
		except Exception:
			frappe.log_error(message=frappe.get_traceback(), title="import_je_disabled")
		return

	# Inicializadas antes del try para que el bloque final no falle con NameError
	res = ''
	error_info = ''
	v_error = False
	v_start_date = now()

	try:
		print("import_je")

		user_val = frappe.conf.db_name
		pass_val = frappe.conf.db_password
		site_db = frappe.conf.db_name

		safe_name = _safe_doc_name(doc.name)

		# Ubicación del archivo con el resultado, se crea un archivo por documento
		source_file = os.path.join(frappe.get_site_path('private', 'files'), 'result_{0}.txt'.format(safe_name))
		source_file_upd = os.path.join(frappe.get_site_path('private', 'files'), 'result_upd_{0}.txt'.format(safe_name))

		# Se incorpora host y port en la conexión (ya no se escapan: no hay shell)
		db_port = '-P{0}'.format(frappe.db.port) if frappe.db.port else ''
		db_host = frappe.db.host or 'localhost'

		# TODO: Se asume que es para una empresa a la vez que se realiza el proceso.
		# TODO: Para hacerlo multiempresa se debe Ajustar ESTRUCTURA DEL ARCHIVO PARA A CAMBIAR TRUNCATE POR DELETE.
		print("import_je 1")
		source_qry_truncate = "TRUNCATE TABLE  `tabJournal_Entry_Temporal`;"
		count_res, res = execute_simple_query(db_host, db_port, user_val, pass_val, site_db, source_qry_truncate, source_file)

		print("res-->", res)
		print("count_res-->", count_res)

		print("import_je 2")

		if not res:

			abs_site_path = os.path.abspath(frappe.get_site_path())
			csv_path = _resolve_import_file(abs_site_path, doc.import_file)

			# Se sustituye ruta del csv completando con el campo doc.import_file
			source_sql = load_qry_journal_entry_temporal(csv_path)
			cmd = _build_mysql_cmd(db_host, db_port, user_val, site_db)
			print("mysql cmd-->", cmd)  # sin contraseña
			_run_mysql(cmd, pass_val, source_file, stdin_text=source_sql)
			count_res, res = read_result(source_file)

			print("res-->", res)
			print("count_res-->", count_res)

			# Validar el registro de la tabla tabJournal_Entry_Temporal
			source_qry_voucher = "Select count(*)\nfrom tabJournal_Entry_Temporal;\n"
			_run_mysql(cmd, pass_val, source_file_upd, stdin_text=source_qry_voucher)
			res_upd, data = read_result_detail(source_file_upd)

			print("res-->", res)
			print("res_upd-->", res_upd)

			if not res and res_upd:

				# Ubicación fija del archivo .sql junto a este módulo
				source_procedure = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'import_journal_entry.sql')

				_run_mysql(cmd, pass_val, source_file, stdin_file=source_procedure)
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

	cmd = _build_mysql_cmd(db_host, db_port, user_val, site_db, extra_args=["-e", source_qry])
	print("mysql cmd-->", cmd)  # sin contraseña
	_run_mysql(cmd, pass_val, source_file)
	count_res, res = read_result_detail(source_file) if sql_detail else read_result(source_file)

	return count_res, res


def load_qry_journal_entry_temporal(je_file_path):

	# Defensa en profundidad: la ruta va dentro de un literal SQL
	if _FORBIDDEN_PATH_CHARS.search(je_file_path or ""):
		raise ValueError("Invalid characters in CSV path")

	qry_load_journal_entry_temporal = """LOAD DATA LOCAL INFILE '{0}'
INTO TABLE tabJournal_Entry_Temporal
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES
(id,finance_book,entry_type,series,company,posting_date,title,total_debit,total_credit,name,party_type,party,account,debit_in_account_currency,credit_in_account_currency);
""".format(je_file_path)

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
