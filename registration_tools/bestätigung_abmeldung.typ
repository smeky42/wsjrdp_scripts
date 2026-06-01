#import "wsjrdp2027.typ": *

#let contract_names = json(bytes(sys.inputs.at("contract_names", default: "[\"\", \"\", \"\"]")))
#let role_id_name = sys.inputs.at("role_id_name", default: "")
#let hitobitoid = sys.inputs.at("hitobitoid", default: fill-in-box(3.9cm))
#let deregistration_issue = sys.inputs.at("deregistration_issue", default: "")
#let full_name = sys.inputs.at("full_name", default: fill-in-box(14cm))
#let birthday_de = sys.inputs.at("birthday_de", default: box(align(bottom, line(length: 4cm, stroke: 0.4pt))))
#let signing_name = sys.inputs.at("signing_name", default: "Ines Höfig")
#let use_signing_name = "full_name" in sys.inputs and "birthday_de" in sys.inputs and "hitobitoid" in sys.inputs
#let form_line_height = if use_signing_name { 1em } else { 2em }
#let accounting_entries = json(bytes(sys.inputs.at("accounting_entries", default: "[]")))
#let cancellation_date_de = sys.inputs.at("cancellation_date_de", default: fill-in-box(4cm))
#let today_de = sys.inputs.at("today_de", default: fill-in-box(4cm))
#let accounting_entry_sum_de = sys.inputs.at("accounting_entry_sum_de", default: fill-in-box(3.9cm))


#show: wsjrdp2027_letter.with(
    body-size: 10pt,
    title-text: [
        #if "role_id_name" in sys.inputs [ #sys.inputs.at("role_id_name")#linebreak()]
        Bestätigung der Abmeldung vom World Scout Jamboree 2027
    ],
    footer-text: [Bestätigung der Abmeldung],
    role-id-name: role_id_name,
)

Datum: #today_de

Hiermit bestätigen wir die Abmeldung zum #cancellation_date_de \
beim Ring deutscher Pfadfinder*innenverbände e.V. (rdp),
Chausseestraße 128/129, 10115 Berlin \
vom Vertrag zur Teilnahme im
deutschen Kontingent zum 26.~World Scout Jamboree 2027 in Polen von

#pad(left: 1.25cm)[
    #set par(first-line-indent: 0cm, spacing: form_line_height)

    #full_name

    geboren am #birthday_de

    Anmeldungs-ID: #hitobitoid

    Entschädigung: #accounting_entry_sum_de #h(1cm) (Anteil des Beitrags der einbehalten wird)

    #if deregistration_issue.len() > 0 [
        Helpdesk-Vorgang Abmeldung: #deregistration_issue
    ]
]

#v(2em)

Für Rückfragen stehen wir Ihnen gerne zur Verfügung.

#v(2em)

Mit freundlichen Grüßen \
#if use_signing_name [#signing_name] else [#box(height: form_line_height)#box(align(bottom, line(length: 8cm, stroke: 0.4pt)))#h(.5em)]
für das Heads of Contingent Team


#if "accounting_entries" in sys.inputs and "accounting_entry_sum_de" in sys.inputs and accounting_entries.len() != 0 [
    #set page(
        background: image("WSJ_Brief_blanko.pdf"),
        margin: (top: 7cm, left: 2.2cm, right: 1.8cm),
        footer: context {
            set text(size: 11pt)
            grid(
                columns: (1fr, 6cm),
                column-gutter: .5cm,
                align: (bottom, bottom + right),
                [#role_id_name], [
                    Kontoauszug (#today_de) \
                    Seite #counter(page).get().first() / #counter(page).final().first()
                ]
            )
        })
    #counter(page).update(1)
    #text(weight: "semibold")[Kontoauszug #role_id_name]

    #text[Datum: #today_de]

    #set text(size: 10pt)
    #table(
        columns: (2.3cm, 1fr, 2.5cm),
        align: (right, left, right),
        stroke: (x, y) => (
            top: 0.4pt + rgb("CCCCCC"),
            bottom: 0.4pt + rgb("CCCCCC"),
            left: none,
            right: none,
        ),
        fill: (x, y) => {
            if y == 0 {
                white // Kopfzeile
            } else if calc.odd(y) {
                rgb("e0e0e0") // Ungerade Zeilen
            } else {
                white         // Gerade Zeilen
            }
        },

        table.header([*Datum*], [*Beschreibung*], [*Betrag*]),
        [], align(right)[Summe], [#accounting_entry_sum_de],
        ..accounting_entries.map(entry => (
            { entry.value_date },
            { block(breakable: false)[
                #set par(spacing: 10pt)
                #entry.description
                #if "short_dbtr" in entry and entry.short_dbtr.len() > 0 [
                    #parbreak()
                    #emph(entry.short_dbtr)
                ]
            ]},
            { entry.amount_de },
        )).flatten()
    )
]
