#let names = json(bytes(sys.inputs.at("names", default: "[\"\", \"\", \"\"]")))
#let person_id_line = sys.inputs.at("person_id_line", default: [(Anmeldungs-ID #box(line(length: 3cm, stroke: 0.4pt)))])
#let full_name = sys.inputs.at("full_name", default: [#box(align(bottom, line(length: 10cm, stroke: 0.4pt)))])
#let birthday_de = sys.inputs.at("birthday_de", default: box(align(bottom, line(length: 4cm, stroke: 0.4pt))))
#let form_line_height = if "full_name" in sys.inputs { 1em } else { 2em }

#import "wsjrdp2027.typ": *

#set document(title: [Stornierung der Registrierung – World Scout Jamboree 2027])

#show: wsjrdp2027_letter.with()

Hiermit #(if names.len() == 1 [storniere ich] else [stornieren wir])
beim Ring deutscher Pfadfinder*innenverbände e.V. (rdp),
Chausseestraße 128/129, 10115 Berlin die Registrierung für das
deutsche Kontingent zum 26. World Scout Jamboree 2027 in Polen von

#par(first-line-indent: 1.25cm, [#box(height: form_line_height)#full_name])
#par(first-line-indent: 1.25cm, [#box(height: form_line_height)geboren am #birthday_de])
#box(height: form_line_height)#person_id_line


#for name in names [
  #signature_line[#name]
]
