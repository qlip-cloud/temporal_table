// Copyright (c) 2022, Mentum and contributors
// For license information, please see license.txt

frappe.ui.form.on('qp_Advanced_Integration', {
	refresh: function(frm) {
		if (!frm.is_new() && frm.doc.status !== "Completed") {
			frm.add_custom_button(__("Do Import"), function() {
				frappe.call({
					doc: frm.doc,
					method: "journal_entry_import",
					callback: function(r) {
						console.log("Do Import Completed");
						if(!r.exc) {
							if(r.message == "okay") {
								frappe.msgprint(__("Completed"))
							} else {
								frappe.msgprint(__("Error! Please see error log"))
							}
						}
					}
				});
		
			});
		}
		frm.refresh_fields();
	},
});
