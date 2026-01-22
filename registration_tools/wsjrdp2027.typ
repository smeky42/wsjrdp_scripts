#let wsjrdp2027_letter(
    body-size: 11pt,
    footer-size: none,
    role-id-name: "",
    title-text: "World Scout Jamboree 2027",
    footer-text: none,
    doc,
) = {
    let footer-size = if footer-size == none { body-size } else { footer-size }
    let footer-text = if footer-text == none { title-text } else { footer-text }
    let footer-right = [#text(size: footer-size)[#footer-text]]
    set document(title: title-text)
    set page(
        background: image("WSJ_Brief_blanko.pdf"),
        margin: (top: 7cm, left: 2.2cm, right: 1.8cm),
        footer: context [
            #set text(size: footer-size)
            #grid(
                columns: (1fr, measure(footer-right).width),
                column-gutter: .5cm,
                align: (bottom, bottom + right),
                [#role-id-name], [#footer-right]
            )
        ])

    set text(font: "Montserrat", size: body-size)
    show heading: set text(weight: "semibold")
    show title: set text(size: body-size, weight: "semibold")
    show title: set block(below: 1.5em)

    title()

    doc
}

#let person_id_line(hitobitoid: "", issue: "", ticket-type: "Stornierung") = {
    if hitobitoid != "" and issue != "" [
        (Anmeldungs-ID #hitobitoid / Vorgang #ticket-type: #issue)
    ] else if hitobitoid != "" [
        (Anmeldungs-ID #hitobitoid)
    ] else [
        (Anmeldungs-ID #box(align(bottom, line(length: 8cm, stroke: 0.4pt))))
    ]
}

#let signature_line(name, columns: (10em, 20em), signature-height: 5em) = {
    grid(
      columns: columns,
      rows: (signature-height, auto),
      column-gutter: 1em,
      row-gutter: 3pt,
      align: (x, y) => if y == 0 { bottom } else { top },
      [#box(width: 1fr, line(length: 100%, stroke: 0.4pt))], [#box(width: 1fr, line(length: 100%, stroke: 0.4pt))],
      [#text(size: 9pt)[Ort, Datum]], [#text(size: 9pt)[#name]],
  )
}

#let signature_lines(names, columns: (10em, 20em), signature-height: 6em) = {
    for name in names {
        signature_line(if name != "" { name } else [Unterschrift], columns: columns, signature-height: signature-height)
    }
}
