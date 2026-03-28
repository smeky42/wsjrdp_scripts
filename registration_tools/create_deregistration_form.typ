#import "wsjrdp2027.typ": *

#let contract_names = json(bytes(sys.inputs.at("contract_names", default: "[\"Unterschrift Teilnehmer*in\", \"\", \"\"]")))
#let hitobitoid = sys.inputs.at("hitobitoid", default: fill-in-box(3.9cm))
#let role_id_name = sys.inputs.at("role_id_name", default: "")
#let betreff_einzahlung = if "role_id_name" in sys.inputs [#role_id_name Beitrag WSJ27] else [\<Anmeldungs-ID\> \<Name\> Beitrag WSJ27]
#let deregistration_issue = sys.inputs.at("deregistration_issue", default: "")
#let full_name = sys.inputs.at("full_name", default: fill-in-box(14cm))
#let birthday_de = sys.inputs.at("birthday_de", default: fill-in-box(4cm))
#let cancellation_date_de = sys.inputs.at("cancellation_date_de", default: fill-in-box(4cm))
#let amount_paid_display = sys.inputs.at("amount_paid_display", default: [#box(height: 1.5em)#box(align(bottom, line(length: 4cm, stroke: 0.4pt)))~€])
#let refund_in_inputs = "refund_amount" in sys.inputs
#let refund_amount_display = sys.inputs.at("refund_amount_display", default: [#box(align(bottom, line(length: 4cm, stroke: 0.4pt)))~€])
#let missing_amount_display = sys.inputs.at("missing_amount_display", default: [#box(align(bottom, line(length: 4cm, stroke: 0.4pt)))~€])
#let refund_iban = sys.inputs.at("refund_iban", default: fill-in-box(10cm, height: 2em))
#let refund_account_holder = sys.inputs.at("refund_account_holder", default: fill-in-box(10cm, height: 2em))
#let contractual_compensation_cents = if "contractual_compensation_display" in sys.inputs {int(sys.inputs.at("contractual_compensation_cents", default: none))} else {none}
#let contractual_compensation_display = sys.inputs.at("contractual_compensation_display", default: [#box(height: 1.5em)#box(align(bottom, line(length: 4cm, stroke: 0.4pt)))~€])
#let actual_compensation_display = sys.inputs.at("actual_compensation_display", default: [#box(height: 1.5em)#box(align(bottom, line(length: 4cm, stroke: 0.4pt)))~€])
#let actual_compensation_cents = if "actual_compensation_display" in sys.inputs {int(sys.inputs.at("actual_compensation_cents", default: none))} else {none}
#let amount_paid_cents = if "amount_paid_cents" in sys.inputs { int(sys.inputs.at("amount_paid_cents", default: none)) } else { none }
#let contractual_compensation_cents = if "contractual_compensation_display" in sys.inputs {int(sys.inputs.at("contractual_compensation_cents", default: none))} else {none}
#let refund_amount_cents = if "refund_amount_cents" in sys.inputs {int(sys.inputs.at("refund_amount_cents", default: none))} else {none}
#let missing_amount_cents = if "missing_amount_cents" in sys.inputs {int(sys.inputs.at("missing_amount_cents", default: none))} else {none}
#let total_fee_cents = int(sys.inputs.at("total_fee_cents", default: 0))
#let has-contractual-compensation-amount = contractual_compensation_cents != none
#let has-actual-compensation-amount = actual_compensation_cents != none
#let has-compensation-amounts = has-actual-compensation-amount and has-contractual-compensation-amount
#let mute-contractual-compensation-amount = has-compensation-amounts and contractual_compensation_cents != actual_compensation_cents
#let rdp = [Ring deutscher Pfadfinder*innenverbände e.V. (rdp)]
#let konto-auszahlung = grid(
    columns: (1.5cm, 4cm, auto),
    align: bottom,
    [], [Kontoinhaber*in:], refund_account_holder,
    [#box(height: 1.5em)], [IBAN:], refund_iban,
)
#let konto-einzahlung = grid(
    columns: (1.5cm, 4cm, auto),
    align: bottom,
    [], [Verwendungszweck:], [#betreff_einzahlung],
    [#box(height: 1.5em)], [Kontoinhaber*in:], [Ring deutscher Pfadfinder'innen],
    [#box(height: 1.5em)], [IBAN:], [DE13 3706 0193 2001 9390 44],
    [#box(height: 1.5em)], [Bank:], [Pax-Bank],
)

#show: wsjrdp2027_letter.with(
    body-size: 10pt,
    title-text: [Abmeldung von der Teilnahme am World Scout Jamboree 2027],
    role-id-name: role_id_name,
    footer-text: [Abmeldung],
)

Hiermit #(if contract_names.len() == 1 [erkläre ich] else [erklären wir])
zum #cancellation_date_de
beim #rdp,
Chausseestraße 128/129, 10115 Berlin
den Rücktritt vom Vertrag zur Teilnahme im deutschen Kontingent
zum 26. World Scout Jamboree 2027 in Polen von

#par(first-line-indent: 1.5cm, [#full_name])
#par(first-line-indent: 1.5cm, [geboren am #birthday_de #h(2em) Anmeldungs-ID: #hitobitoid])



#v(.6em)
#if has-contractual-compensation-amount [
    #set text(fill: gray)
    #(if contract_names.len() == 1 [Mir] else [Uns])
    ist bekannt, dass dem #rdp laut Abschnitt 7.2 der
    Teilname- und Reisebedinungen eine Entschädigung in Höhe von 
    #contractual_compensation_display
    zusteht.]

#if actual_compensation_cents != none and contractual_compensation_cents != none and (actual_compensation_cents < contractual_compensation_cents) [
  #(if contract_names.len() == 1 [Ich nehme] else [Wir nehmen]) das Angebot an,
  für den Rücktritt vom Teilnahmevertrag dem #rdp eine Entschädigung in Höhe von
  #actual_compensation_display
  zu leisten.
] else [
  #(if contract_names.len() == 1 [Ich verpflichte mich] else [Wir verpflichten uns]),
  dem #rdp eine Entschädigung in Höhe von
  #actual_compensation_display
  zu leisten.
]

Bisher #(if contract_names.len() == 1 [habe ich] else [haben wir]) Teilnahmebeiträge
in Höhe von #amount_paid_display bezahlt.

#v(.6em)
#if refund_amount_cents == none [
    Falls es eine Rückzahlung gibt, soll diese auf folgendes Konto überwiesen werden: #konto-auszahlung

    #v(.6em)
    Ausstehende Beiträge werden wir auf das Jamboree-Konto überweisen: #konto-einzahlung
] else if missing_amount_cents > 0 [
    Den ausstehenden Betrag von #missing_amount_display
    #(if contract_names.len() == 1 [werde ich] else [werden wir]) auf das Jamboree-Konto überweisen: #konto-einzahlung
] else [
    Die Rückzahlung von
    #refund_amount_display
    soll auf folgendes Konto überwiesen werden: #konto-auszahlung]



#signature_lines(contract_names, signature-height: 3em)
