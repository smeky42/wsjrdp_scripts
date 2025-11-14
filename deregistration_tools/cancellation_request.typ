#let names = json(bytes(sys.inputs.names))

#import "wsjrdp2027.typ": *

#set document(title: [Stornierung der Registrierung – World Scout Jamboree 2027])

#show: wsjrdp2027_letter.with()

Hiermit #(if names.len() == 1 [storniere ich] else [stornieren wir])
beim Ring deutscher Pfadfinder*innenverbände e.V. (rdp),
Chausseestraße 128/129, 10115 Berlin die Registrierung für das
deutsche Kontingent zum 26. World Scout Jamboree 2027 in Polen von

#block(inset: (left: 1.25cm))[
  #sys.inputs.full_name \
  geboren am #sys.inputs.birthday_de \
]

#sys.inputs.person_id_line


#for name in names [
  #signature_line[#name]
]
