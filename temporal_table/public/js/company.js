frappe.ui.form.on("Company", "refresh", function(frm) {
    frm.add_custom_button(__("Advanced Integration"), function() {
            frappe.set_route("List", "qp_Advanced_Integration",{"import_type": 'qp_je'});
        });
    });