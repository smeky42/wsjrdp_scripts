#let wsjrdp2027_letter(doc) = {
  set page(
    background: image("WSJ_Brief_blanko.pdf"),
    margin: (top: 8cm),
  )

  set text(font: "Montserrat", size: 11pt)
  show heading: set text(weight: "semibold")
  show title: set text(size: 11pt, weight: "semibold")
  show title: set block(below: 1.5em)

  title()

  doc
}

#let person_id_line(hitobitoid: "", issue: "") = {
  if hitobitoid != "" and issue != "" [
    (Anmeldungs-ID #hitobitoid / Vorgang Stornierung: #issue)
  ] else if hitobitoid != "" [
    (Anmeldungs-ID #hitobitoid)
  ] else [
    (Anmeldungs-ID #box(align(bottom, line(length: 8cm, stroke: 0.4pt))))
  ]
}

#let signature_line(name) = {
  grid(
    columns: (4cm, 6.5cm),
    rows: (3cm, auto),
    column-gutter: .5cm,
    row-gutter: 3pt,
    align: (x, y) => if y == 0 { bottom } else { top },
    [#box(width: 1fr, line(length: 100%, stroke: 0.4pt))], [#box(width: 1fr, line(length: 100%, stroke: 0.4pt))],
    [#text(size: 9pt)[Ort, Datum]], [#text(size: 9pt)[#name]],
  )
}

#let signature_lines(names) = {
  for name in names {
    signature_line(if name != "" { name } else [Unterschrift])
  }
}
