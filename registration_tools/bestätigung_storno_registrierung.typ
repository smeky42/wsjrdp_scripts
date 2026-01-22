#let contract_names = json(bytes(sys.inputs.at("contract_names", default: "[\"\", \"\", \"\"]")))
#let role_id_name = sys.inputs.at("role_id_name", default: "")
#let hitobitoid = sys.inputs.at("hitobitoid", default: "")
#let deregistration_issue = sys.inputs.at("deregistration_issue", default: "")
#let full_name = sys.inputs.at("full_name", default: [#box(align(bottom, line(length: 10cm, stroke: 0.4pt)))])
#let birthday_de = sys.inputs.at("birthday_de", default: box(align(bottom, line(length: 4cm, stroke: 0.4pt))))
#let signing_name = sys.inputs.at("signing_name", default: "Ines Höfig")
#let use_signing_name = "full_name" in sys.inputs and "birthday_de" in sys.inputs and "hitobitoid" in sys.inputs
#let form_line_height = if use_signing_name { 1em } else { 2em }

#import "wsjrdp2027.typ": *

#show: wsjrdp2027_letter.with(
    body-size: 10pt,
    title-text: [Bestätigung der Stornierung der Registrierung – World Scout Jamboree 2027],
    footer-text: [Bestätigung der \ Stornierung der Registrierung],
    role-id-name: role_id_name,
)

Hiermit bestätigen wir die Stornierung der Registrierung
beim Ring deutscher Pfadfinder*innenverbände e.V. (rdp),
Chausseestraße 128/129, 10115 Berlin die Registrierung für das
deutsche Kontingent zum 26.~World Scout Jamboree 2027 in Polen von

#par(first-line-indent: 1.25cm, [#box(height: form_line_height)#full_name])
#par(first-line-indent: 1.25cm, [#box(height: form_line_height)geboren am #birthday_de])
#box(height: form_line_height)#person_id_line(hitobitoid: hitobitoid, issue: deregistration_issue, ticket-type: "Stornierung")

#v(2em)

Schade, dass die Teilnahme nicht möglich ist.

Für Rückfragen stehen wir Ihnen gerne zur Verfügung.

#v(2em)

Mit freundlichen Grüßen \
#if use_signing_name [#signing_name] else [#box(height: form_line_height)#box(align(bottom, line(length: 8cm, stroke: 0.4pt)))#h(.5em)]
für das Heads of Contingent Team
