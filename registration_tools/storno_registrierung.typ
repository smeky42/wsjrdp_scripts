#let contract_names = json(bytes(sys.inputs.at("contract_names", default: "[\"\", \"\", \"\"]")))
#let hitobitoid = sys.inputs.at("hitobitoid", default: "")
#let deregistration_issue = sys.inputs.at("deregistration_issue", default: "")
#let full_name = sys.inputs.at("full_name", default: [#box(align(bottom, line(length: 10cm, stroke: 0.4pt)))])
#let birthday_de = sys.inputs.at("birthday_de", default: box(align(bottom, line(length: 4cm, stroke: 0.4pt))))
#let form_line_height = if "full_name" in sys.inputs { 1em } else { 1.5em }
#let amount_paid = sys.inputs.at("amount_paid", default: [#box(align(bottom, line(length: 4cm, stroke: 0.4pt))) €])
#let refund_in_inputs = "refund_amount" in sys.inputs
#let refund_amount = sys.inputs.at("refund_amount", default: [#box(align(bottom, line(length: 4cm, stroke: 0.4pt))) €])
#let refund_iban = sys.inputs.at("refund_iban", default: [#box(align(bottom, line(length: 10cm, stroke: 0.4pt)))])
#let refund_account_holder = sys.inputs.at("refund_account_holder", default: [#box(align(bottom, line(length: 10cm, stroke: 0.4pt)))])
#let refund_show = json(bytes(sys.inputs.at("refund_show", default: "true")))


#import "wsjrdp2027.typ": *

#set document(title: [Stornierung der Registrierung – World Scout Jamboree 2027])

#show: wsjrdp2027_letter.with()

Hiermit #(if contract_names.len() == 1 [storniere ich] else [stornieren wir])
beim Ring deutscher Pfadfinder*innenverbände e.V. (rdp),
Chausseestraße 128/129, 10115 Berlin die Registrierung für das
deutsche Kontingent zum 26. World Scout Jamboree 2027 in Polen von

#par(first-line-indent: 1.25cm, [#box(height: form_line_height)#full_name])
#par(first-line-indent: 1.25cm, [#box(height: form_line_height)geboren am #birthday_de])
#box(height: form_line_height)#person_id_line(hitobitoid: hitobitoid, issue: deregistration_issue)

#v(1.5em)
Bisher sind Teilnahmebeträge in Höhe von #box(height: form_line_height)#amount_paid bezahlt worden.

#if refund_show [#v(0pt)
Davon werden #box(height: form_line_height)#refund_amount auf folgendes Konto zurückerstattet:

#grid(
  columns: (0.75cm, auto, auto),
  rows: (2 * form_line_height, 2 * form_line_height),
  column-gutter: .5cm,
  align: (x, y) => bottom,
  [], [IBAN:], refund_iban,
  [], [Kontoinhaber*in:], refund_account_holder,
)
]

#signature_lines(contract_names)
