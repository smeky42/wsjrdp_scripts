# Bulk-Upload Polen 2027 – Feld-Mapping

Mapping der Spalten aus `Bulk-Upload-Template.XLSX` (Blatt **Import Data**) auf die Daten in hitobito (`people`-Tabelle, lokale Postgres-DB).
Grundlage: fachliche Notizen aus [bulk-upload-fields.html](bulk-upload-fields.html), DB-Schema aus `hitobito_wsjrdp_2027` (`db/schema.rb`, `app/models/wsjrdp_2027/person/jamboree_data.rb`).

## Legende

| Symbol | Bedeutung |
|---|---|
| ✅ | direkt aus der DB übernehmen |
| 🔁 | aus der DB, mit Umwandlung (siehe Spalte „Umwandlung“ bzw. Anhang) |
| 📌 | Konstante (siehe [Konstanten](#konstanten)) |
| ⬜ | leer lassen |
| ❓ | offen / TODO – Quelle oder Regel noch zu klären |

- `jd.<key>` = Schlüssel im JSONB-Feld `people.jamboree_data` (z. B. `jd.nationality` = `people.jamboree_data->>'nationality'`).
- `jd.<bool>`-Felder: angehakt = `true`, nicht angehakt = Schlüssel fehlt (→ als `false` behandeln).
- **Pflicht**: `x` = Pflichtfeld laut Template/Notizen, `Visa` = nur Pflicht, wenn ein Visum benötigt wird.
- **Rollen**: für wen das Feld befüllt wird. `alle` = YP, UL, IST, CMT. `Adult` = UL, IST, CMT.

## Aufbau des Templates

| Eigenschaft | Wert |
|---|---|
| Datenblatt | `Import Data` |
| Kopfzeile (Spaltennamen) | Zeile 2 |
| Beschreibungszeile | Zeile 3 |
| Erste Datenzeile | Zeile 5 (Dropdown-Validierungen gelten für Zeilen 5–205) |
| Spalten | `A` bis `GO` (197 Spalten) |
| Dropdown-Werte | versteckte Blätter (Spalte B), siehe [Anhang A](#anhang-a--wertelisten-db--template) |
| Datumsformat | **`yyyy/mm/dd`** (von der Vorlage erlaubt, eindeutig; bestätigt) |

> Max. 201 Datenzeilen pro Datei (Validierung bis Zeile 205) → ggf. mehrere Dateien erzeugen (z. B. pro Rolle oder pro Unit).

## Personenauswahl

| Kriterium | Regel |
|---|---|
| Rolle | `people.wsj_role` ∈ `YP`, `UL`, `IST`, `CMT`. JPT und EXT werden nie exportiert. Bei `wsj_role = NULL` die Rolle aus `payment_role` ableiten (wie `effective_wsj_role`) ❓ |
| Status | nur `people.status = 'confirmed'` |

## Konstanten

| Name | Wert |
|---|---|
| `CONTINGENT_COUNTRY` | `Germany` |
| `CONTINGENT_NSO` | `Germany - Ring deutscher Pfadfinder*innenverbände` (identisch mit `Configuration › Organisation Name`) |
| `NATIVE_LANGUAGE` | `German` ❓ (Notiz sagt „Deutsch“; Template erwartet vermutlich Englisch) |
| `RINGEBUERO_ADDRESS` | Address 1: `c/o Ring deutscher Pfadfinder*innenverbände e.V. (rdp)` · Address 2: `Chausseestraße 128/129` · Postcode: `10115` · City: `Berlin` · State: `Berlin` · Country: `Germany` |
| `CMT_EMERGENCY_CONTACT` | Vorname: `Notfalltelefon` · Nachname: `Deutsches Kontingent` · E-Mail: `notfallkontakt@worldscoutjamboree.de` · Telefon: `CONTINGENT_PHONE` · Adresse: `RINGEBUERO_ADDRESS` |
| `CONTINGENT_PHONE` | `+4917TODO` (für AA, BO, DV; Platzhalter wie vorgegeben) |

---

## Mapping

### Allgemein

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| A | Sub Category | `wsj_role` | siehe [A.1 Rolle](#a1-rolle--sub-category) | alle | x | 🔁 |
| B | Please agree to the GDPR agreement | `jd.gdpr_agreement` | `true`→`Yes`, sonst `No` | alle | x | 🔁 |
| C | Contingent Country Name | – | `CONTINGENT_COUNTRY` | alle | x | 📌 |
| D | Contingent NSO Name | – | `CONTINGENT_NSO` | alle | x | 📌 |
| E | Patrol ID | `patrol` | | YP, UL | | ✅ |

### Person

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| F | First Name | `first_name` | Template: „one name only“ | alle | x | ✅ |
| G | Last Name | `last_name` | | alle | x | ✅ |
| H | Surname at birth | – | | | | ⬜ |
| I | Known as name | `jd.known_as_name` | leer → `nickname` → `first_name` (wie `known_as_name_or_default`) | alle | | 🔁 |
| J | Mother's First Name | `jd.mothers_first_name` | | Adult | x | ✅ |
| K | Father's First Name | `jd.fathers_first_name` | | Adult | x | ✅ |
| L | Mother's surname at birth | `jd.mothers_surname_at_birth` | | Adult | x | ✅ |
| M | Date of Birth | `birthday` | Datumsformat (s. o.) | alle | x | 🔁 |
| N | Place of Birth | `jd.place_of_birth` | | Adult | x | ✅ |
| O | Country of birth | `jd.country_of_birth` | deutscher Freitext (z. B. `Deutschland`) → Name aus Liste `Country`, siehe [A.3](#a3-länder-und-nationalität) | alle | x | 🔁 |
| P | Gender (please select) | `gender` | siehe [A.2 Gender](#a2-gender) | alle | x | 🔁 |
| Q | Religion(s) | – | | | | ⬜ |
| R | Nationality | `jd.nationality`, Fallback `passport_nationality` | beide sind deutscher Freitext (z. B. `Deutsch`) → Name aus Liste `Nationalities`, siehe [A.3 Länder](#a3-länder-und-nationalität); bei mehreren nur die erste | alle | x | 🔁 |
| S | National Identity Number | – | | | | ⬜ |

### Adresse & Kontakt

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| T | Address 1 | `street`, `housenumber` | `"{street} {housenumber}"` | alle | x | 🔁 |
| U | Address 2 | (`address_care_of`) | optional; Notiz lässt leer | | | ⬜ |
| V | City | `town` | | alle | x | ✅ |
| W | State County | – | | | | ⬜ |
| X | Postcode | `zip_code` | | alle | x | ✅ |
| Y | Country | `country` (ISO-Code) | ISO → Name aus Liste `Country`, siehe [A.3](#a3-länder-und-nationalität) | alle | x | 🔁 |
| Z | Email Address | `email` | immer die primäre E-Mail, auch bei YP ohne Visum (dient später zur Zuordnung der Datensätze) | alle | x | ✅ |
| AA | Phone number (+XX) | – | `CONTINGENT_PHONE` | Adult | | 📌 |

### Begleitperson (Accompanying Carer, AC)

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| AB | Do you need an Adult … carer (AC)? | – | immer `No`; die seltenen Fälle (Units mit 5 ULs) werden von Hand nachgetragen | YP | x | 📌 |
| AC | AC - First name(s) | – | wird von Hand nachgetragen | YP | | ⬜ |
| AD | AC - Surname | – | wird von Hand nachgetragen | YP | | ⬜ |
| AE | AC - Email address | – | wird von Hand nachgetragen | YP | | ⬜ |

### Sprachen

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| AF | What is your native language? | – | `NATIVE_LANGUAGE` | alle | x | 📌 |
| AG–AJ | English reading / writing / speaking / listening | `jd.english_reading`, `jd.english_writing`, `jd.english_speaking`, `jd.english_listening` | siehe [A.4 Sprachniveau](#a4-sprachniveau) | alle | x | 🔁 |
| AK–AN | French reading / writing / speaking / listening | `jd.french_*` | [A.4](#a4-sprachniveau) | Adult | x | 🔁 |
| AO–AR | Spanish reading / writing / speaking / listening | `jd.spanish_*` | [A.4](#a4-sprachniveau) | Adult | x | 🔁 |
| AS–AV | Arabic reading / writing / speaking / listening | `jd.arabic_*` | [A.4](#a4-sprachniveau) | Adult | x | 🔁 |

### Komfort / Aktivitäten

Alle: `true` → `I do feel comfortable` (Ausnahme **AZ**: `I feel comfortable`), sonst `I do not feel comfortable`. Ein fehlender Schlüssel gilt als `false`.

| Sp. | Template-Feld | DB-Quelle | Rollen | Status |
|---|---|---|---|---|
| AW | Comfortable being on Water? (Sailing/Kayaking) | `jd.comfortable_on_water` | alle | 🔁 |
| AX | Comfortable being in Water? (Swimming) | `jd.comfortable_in_water` | alle | 🔁 |
| AY | Comfortable without balance? | `jd.comfortable_without_balance` | alle | 🔁 |
| AZ | Comfortable in a large crowd? | `jd.comfortable_in_crowd` | alle | 🔁 |
| BA | Comfortable with flashing lights? | `jd.comfortable_flashing_lights` | alle | 🔁 |
| BB | Comfortable with strong noise? | `jd.comfortable_loud_noise` | alle | 🔁 |
| BC | Comfortable in a confined space? | `jd.comfortable_confined_space` | alle | 🔁 |

### Sonstiges

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| BD | Prior WSJ Experience | `jd.prior_wsj_experience` | Freitext | Adult | x | ✅ |
| BE | T-Shirt Size WSJ | `shirt_size` | siehe [A.5 T-Shirt](#a5-t-shirt) | Adult | x | 🔁 |
| BF | Pre-selected IST for Contingent Pavilions | Tag `Preselected-IST`, `primary_group_id = 45` (BMT) | Tag vorhanden **oder** in Gruppe BMT → `Yes`, sonst `No` | IST | | 🔁 |
| BG | Pre-selected IST for Food Houses | Tag `Preselected-IST`, `primary_group_id = 45` (BMT) | gleiche Regel wie BF | IST | | 🔁 |
| BH | Car registration number | `jd.vehicle_registration_number` | | Adult | | ✅ |

### Erziehungsberechtigte (nur YP)

Nur für YP befüllen, bei allen anderen leer lassen. Quelle ist Kontakt A (`additional_contact_*_a`), die Adresse ist immer `RINGEBUERO_ADDRESS`.

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Status |
|---|---|---|---|---|
| BI | Legal guardian - Given name | `additional_contact_name_a` | Name aufteilen: alles bis auf das letzte Wort | 🔁 |
| BJ | Legal guardian - Surname | `additional_contact_name_a` | letztes Wort | 🔁 |
| BK | Legal guardian - Relationship | – | `Parent` | 📌 |
| BL | Parent/Guardian Other | – | nur bei `Other` | ⬜ |
| BM | Legal Guardian - Nationality | – | ❓ (z. B. `Germany` oder leer) | ❓ |
| BN | Legal Guardian - Email address | `additional_contact_email_a` | | ✅ |
| BO | Legal Guardian - Phone number | – | `CONTINGENT_PHONE` | 📌 |
| BP | Legal Guardian - Language | – | ❓ (`German`?) | ❓ |
| BQ | Home address 1 | – | `c/o Ring deutscher Pfadfinder*innenverbände e.V. (rdp)` | 📌 |
| BR | Home address 2 | – | `Chausseestraße 128/129` | 📌 |
| BS | City | – | `Berlin` | 📌 |
| BT | State | – | `Berlin` | 📌 |
| BU | Postcode | – | `10115` | 📌 |
| BV | Country | – | `Germany` | 📌 |

### Reisedokument

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| BW | Travel Document Type | `jd.travel_document_type` | siehe [A.6 Reisedokument](#a6-reisedokument) | alle | x | 🔁 |
| BX | Travel Doc Other | `jd.travel_document_type` | bei `official_passport` (Hitobito: Personalausweis) `German ID card (Personalausweis)`; bei `other_travel_document` manuell klären | alle | x | 🔁 |
| BY | Travel Document Number | `jd.travel_document_number` | | alle | x | ✅ |
| BZ | Travel Document Date of Issue | `jd.travel_document_issue_date` | Datumsformat | alle | x | 🔁 |
| CA | Travel Document Expiration Date | `jd.travel_document_expiration_date` | Datumsformat | alle | x | 🔁 |
| CB | Travel Document Issued By (Country) | `jd.travel_document_country` | deutscher Freitext (z. B. `Deutschland`) → Name aus Liste `Country`, siehe [A.3](#a3-länder-und-nationalität) | alle | x | 🔁 |

### Visum

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| CC | Do you need a Visa to enter Poland? | `jd.visa_needed` | siehe [A.7 Visum](#a7-visum) | alle | x | 🔁 |
| CD | Visa Information | `jd.visa_information` | | alle | Visa | ✅ |
| CE | Place of Birth (Visa) | `jd.place_of_birth` | nur wenn `visa_needed = yes_need_support` | alle | Visa | 🔁 |
| CF | Country of Birth (Visa) | `jd.country_of_birth` | nur wenn `visa_needed = yes_need_support` | alle | Visa | 🔁 |
| CG–CL | Nationalität bei Geburt, weitere Nationalitäten, Geschlecht, Familienstand, Studierendenstatus | `jd.visa_nationality_at_birth`, `jd.visa_other_nationalities`, `jd.visa_gender_as_in_travel_document`, `jd.visa_civil_status`, `jd.visa_civil_status_other`, `jd.visa_current_student_status` | Dropdown-Werte bei CI, CJ und CL übersetzen | alle | Visa (außer CH, CK) | 🔁 |
| CM | Family member citizen of CH/EU/EEA? | `jd.visa_family_eu_citizen_relationship` | bei vorhandener Beziehung `Yes`, sonst leer (nicht `No` raten) | alle | x laut Notiz | 🔁 |
| CN–CT | Employer's / Educational institution's (Name, Adresse 1/2, City, Postcode, State, Country) | `jd.visa_employer_name`, `jd.visa_employer_address_1`, `jd.visa_employer_address_2`, `jd.visa_employer_city`, `jd.visa_employer_postcode`, `jd.visa_employer_state`, `jd.visa_employer_country` | Land übersetzen | alle | Visa (CN) | 🔁 |
| CU–CV | Beziehung zu EU-/EWR-/CH-Staatsangehörigen | `jd.visa_family_eu_citizen_relationship`, `jd.visa_family_eu_citizen_relation` | CU als Dropdown-Wert übersetzen | alle | | 🔁 |
| CW–CX | Abweichendes Wohnsitzland | `jd.visa_residence_other_than_nationality`, `jd.visa_residence_country` | Ja/Nein und Land übersetzen | alle | Visa (CW) | 🔁 |
| CY | Schengen Member State of first entry | `jd.visa_schengen_first_entry` | Land übersetzen | alle | Visa | 🔁 |
| CZ–DA | Fingerabdrücke und ggf. Datum/Visumaufklebernummer | `jd.visa_fingerprints_collected`, `jd.visa_fingerprints_date`, `jd.visa_fingerprints_visa_number` | Ja/Nein; Datum und Nummer in DA mit `; ` verbinden | alle | | 🔁 |

> Die Visa-Details werden in hitobito nur bei `visa_needed = yes_need_support` gespeichert und auch nur dann exportiert. Unbekannte Dropdown-Werte werden gewarnt und nicht exportiert; nicht erhobene Pflichtangaben müssen weiterhin geklärt werden.

### Anreise / Abreise

| Sp. | Template-Feld | Status |
|---|---|---|
| DB | Arrival and Departure Information (Überschrift) | ⬜ |
| DC | Arrival | ⬜ |
| DD | Arrival Location (Dropdown, Pflicht) | `jd.arrival_location`, Wert aus der Vorlagenliste |
| DE | Arrival Date | `jd.arrival_date`, `yyyy/mm/dd` |
| DF | Arrival time | `jd.arrival_time` |
| DG | Arrival Type (Dropdown, Pflicht) | `jd.arrival_type`, Wert aus der Vorlagenliste |
| DH | Arrival Number | `jd.arrival_number` |
| DI | Departure | ⬜ |
| DJ | Departure Location (Dropdown, Pflicht) | `jd.departure_location`, Wert aus der Vorlagenliste |
| DK | Departure Date | `jd.departure_date`, `yyyy/mm/dd` |
| DL | Departure Time | `jd.departure_time` |
| DM | Departure Type (Dropdown, Pflicht) | `jd.departure_type`, Wert aus der Vorlagenliste |
| DN | Departure Number | `jd.departure_number` |

Die Reiseangaben sind in hitobito auf der Zusatzdatenseite nur mit `:log` für die Person sichtbar; Bearbeitung erfordert zusätzlich `:edit`.

### Notfallkontakt

Bei allen Personen ist der Notfallkontakt der feste CMT-Kontakt (`CMT_EMERGENCY_CONTACT`), nicht der private Kontakt.

| Sp. | Template-Feld | Wert | Pflicht | Status |
|---|---|---|---|---|
| DO | Emergency Contact (Überschrift) | | | ⬜ |
| DP | First (Given) Name | `Notfalltelefon` | x | 📌 |
| DQ | Surname | `Deutsches Kontingent` | x | 📌 |
| DR | Relationship | `Other` ❓ (Liste: Partner, Parent, Family, Member, Friend, Colleague, Other) | x | 📌 |
| DS | Relationship Other | `Contingent Management Team` ❓ | x | 📌 |
| DT | Nationality | `Germany` | x | 📌 |
| DU | Email Address | `notfallkontakt@worldscoutjamboree.de` | x | 📌 |
| DV | Phone Number | `CONTINGENT_PHONE` (derzeit `+4917TODO`) | x | 📌 |
| DW | Address 1 | `c/o Ring deutscher Pfadfinder*innenverbände e.V. (rdp)` | x | 📌 |
| DX | Address 2 | `Chausseestraße 128/129` | | 📌 |
| DY | City | `Berlin` | x | 📌 |
| DZ | State | `Berlin` | x | 📌 |
| EA | Postcode | `10115` | x | 📌 |
| EB | Country | `Germany` | x | 📌 |

### Ernährung

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Rollen | Pflicht | Status |
|---|---|---|---|---|---|---|
| EC | Health & Diet (Überschrift) | | | | | ⬜ |
| ED | Any dietary requirements or preferences? | `diet`, `jd.dietary_requirements` | `Yes`, wenn `diet ≠ omnivorous` oder `dietary_requirements` gesetzt ist, sonst `No` | alle | x | 🔁 |
| EE | Select dietary requirements (special diet II) | `diet` | siehe [A.8 Ernährung](#a8-ernährung) | alle | | 🔁 |
| EF | Specify special diet (special diet III) | `jd.dietary_requirements`, `jd.dietary_details` | beide Freitexte mit `; ` verbinden (leere weglassen) | alle | | 🔁 |

> `jd.dietary_requirements` ist Freitext und kann der Spalte `diet` widersprechen (z. B. `diet = omnivorous` mit Freitext `Vegan`). Maßgeblich für EE ist `diet`, der Freitext landet in EF.

### Allergien

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Status |
|---|---|---|---|---|
| EG | Do you have any allergies? | alle `jd.allergy_<x>` | `Yes`, wenn mindestens eine `true` ist, sonst `No` | 🔁 |

Pro Allergie gibt es zwei Spalten. „Allergie?“ wird `true` → `Yes`, sonst `No`. „Schweregrad“ wird nach [A.9 Schweregrad](#a9-allergie-schweregrad) umgesetzt und nur befüllt, wenn die Allergie `Yes` ist.

| Sp. Ja/Nein | Sp. Schweregrad | Template-Allergie | DB-Schlüssel |
|---|---|---|---|
| EH | EI | Milk (Cows Milk) | `jd.allergy_milk`, `jd.allergy_milk_severity` |
| EJ | EK | Casein | `jd.allergy_casein`, `…_severity` |
| EL | EM | Egg | `jd.allergy_egg`, `…_severity` |
| EN | EO | Peanuts | `jd.allergy_peanut`, `…_severity` |
| EP | EQ | Tree Nuts | `jd.allergy_tree_nuts`, `…_severity` |
| ER | ES | Cereals containing gluten | `jd.allergy_gluten`, `…_severity` |
| ET | EU | Soybeans | `jd.allergy_soy`, `…_severity` |
| EV | EW | Lupin | `jd.allergy_lupin`, `…_severity` |
| EX | EY | Mustard | `jd.allergy_mustard`, `…_severity` |
| EZ | FA | Crustaceans | `jd.allergy_crustaceans`, `…_severity` |
| FB | FC | Sesame | `jd.allergy_sesame`, `…_severity` |
| FD | FE | Molluscs | `jd.allergy_molluscs`, `…_severity` |
| FF | FG | Sulphur Dioxide & Sulphites | `jd.allergy_sulfite`, `…_severity` |
| FH | FI | Fruit | `jd.allergy_fruit`, `…_severity` |
| FJ | FK | Alliums | `jd.allergy_allium`, `…_severity` |
| FL | FM | Fish | `jd.allergy_fish`, `…_severity` |
| FN | FO | Shellfish | `jd.allergy_seafood`, `…_severity` |

### Medizin

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Status |
|---|---|---|---|---|
| FP | Any chronic diseases? | `medical_preexisting_conditions` | nicht leer → `Yes`, sonst `No` | 🔁 |
| FQ | Specify chronic disease(s) | `medical_preexisting_conditions` | Freitext, unverändert | ✅ |
| FR | Medicines consumed regularly | `medical_continuous_medication` | Freitext, unverändert | ✅ |
| FS | Medication needs refrigeration? | `jd.refrigerated_medication_needed` | `true` → `Yes`, sonst `No` | 🔁 |
| FT | Religious beliefs influencing health decisions | `jd.religious_health_beliefs` | Freitext, Englisch | ✅ |
| FU | Vaccinated with Tetanus? | `medical_stiko_vaccinations` | `true` → `Yes`, sonst `No` | 🔁 |
| FV | Tetanus Vaccination when | – | | ⬜ |
| FW | Vaccinated against Diphteria? | `medical_stiko_vaccinations` | `true` → `Yes`, sonst `No` | 🔁 |
| FX | Diphtheria Vaccination when | – | | ⬜ |
| FY | Blood type | – | Notiz: wird ignoriert | ⬜ |
| FZ | Special needs regarding medical equipment? | `jd.medical_equipment_needs` (Array) | alle Werte nach [A.10 Med. Geräte](#a10-medizinische-geräte) übersetzen und mit `; ` verbinden. ⚠️ Bei mehreren Werten passt das nicht zur Dropdown-Validierung | 🔁 |
| GA | Specify needs re medical equipment | `jd.medical_equipment_details` | Freitext (nur gesetzt bei `other`) | ✅ |

### Mobilität

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Status |
|---|---|---|---|---|
| GB | Any mobility restrictions? | `jd.mobility_restricted` | `true` → `Yes`, sonst `No` | 🔁 |
| GC | Using a Wheelchair? | `jd.uses_wheelchair` | `true` → `Yes`, sonst `No` | 🔁 |
| GD | Wheelchair Manual or Electric? | `jd.wheelchair_type` | `manual` → `Manual`, `electric` → `Electric` | 🔁 |
| GE | Using a Cane/Stick/Frame? | `jd.uses_mobility_frame` | `true` → `Yes`, sonst `No` | 🔁 |
| GF | Any other mobility needs? | `jd.other_mobility_needs` | `true` → `Yes`, sonst `No` | 🔁 |
| GG | Further Mobility Needs | `jd.other_mobility_details` | Freitext | ✅ |
| GH | Is a larger tent required? | – | (ggf. `Yes`, wenn `uses_wheelchair`) ❓ | ⬜ ❓ |

### Nicht sichtbare Beeinträchtigungen

| Sp. | Template-Feld | DB-Quelle | Umwandlung | Status |
|---|---|---|---|---|
| GI | Any non-visible disability? | `jd.non_visible_disability` | `true` → `Yes`, sonst `No` | 🔁 |
| GJ | Blind or low-vision? | – | | ⬜ |
| GK | Other significant visual impairment? | – | | ⬜ |
| GL | Hard of hearing or deaf? | – | | ⬜ |
| GM | Struggle with loud noises, bright lights, crowds? | `jd.sensory_sensitivities` | `true` → `Yes`, sonst `No` | 🔁 |
| GN | Other non-visible disability needing support? | `jd.non_visible_disability` | `true` → `Yes`, sonst `No` | 🔁 |
| GO | Specify condition/disability and adaptations | `jd.non_visible_disability_details` | Freitext | ✅ |

---

## Anhang A – Wertelisten (DB → Template)

Die Template-Werte stammen aus den versteckten Dropdown-Blättern und müssen **exakt** so geschrieben werden.

### A.1 Rolle → Sub Category

| `wsj_role` | Template |
|---|---|
| `YP` | `Youth Participants` |
| `UL` | `Adult Patrol Leaders` |
| `IST` | `International Service Team` |
| `CMT` | `Contingent Management Team` |
| `CMT` + Tag `HoC` ❓ | `Head of Contingent` |
| `JPT`, `EXT` | nicht exportieren |

### A.2 Gender

| `gender` | Template |
|---|---|
| `w` | `Female` |
| `m` | `Male` |
| `d` | `Other` |
| `NULL` / leer | `Prefer not to say` |

### A.3 Länder und Nationalität

Das Template hat **zwei unterschiedliche Listen**: `Country` (222 Einträge, für Y, CB, O …) und `Nationalities` (209 Einträge, für R). Die Schreibweisen weichen voneinander ab.

Die DB enthält drei Arten von Länderangaben:

- ISO-Codes in `people.country`
- deutsche Adjektive in `jd.nationality` und `passport_nationality`
- deutsche Ländernamen in `jd.country_of_birth` und `jd.travel_document_country`

Alle drei brauchen eine Übersetzung.

| ISO (`country`) | Adjektiv (`nationality`) | Land (`country_of_birth`, `travel_document_country`) | `Country` (Y, O, CB) | `Nationalities` (R) |
|---|---|---|---|---|
| `DE` | `Deutsch` | `Deutschland` | `Germany` | `Germany` |
| `AT` | `Österreichisch`, `Österreich` | `Österreich` | `Austria` | `Austria` |
| `CH` | `Schweizerisch` | `Schweiz` | `Switzerland` | `Switzerland` |
| `NL` | `Niederländisch` | `Niederlande` | `Netherlands` | `Netherlands` |
| `PL` | `Polnisch` | `Polen` | `Poland` | `Poland` |
| `SE` | `Schwedisch` | `Schweden` | `Sweden` | `Sweden` |
| `US` | `US-Amerikanisch` | `USA`, `Vereinigte Staaten` | `United States of America` | `United States` |
| `GB` | `Britisch` | `Großbritannien`, `Vereinigtes Königreich` | `United Kingdom` | `United Kingdom` |
| `MY` | `Malaysisch` | `Malaysia` | `Malaysia` | `Malaysia` |

- Die Übersetzung ignoriert Groß- und Kleinschreibung und Leerzeichen am Rand (z. B. `deutsch`).
- Unbekannte Werte werden nicht geraten. Das Skript lässt die Zelle leer und schreibt den Wert in eine Warnliste, damit die Tabelle ergänzt werden kann.
- `people.country = NULL` → `Germany` ❓
- Bei mehreren Nationalitäten (z. B. `Deutsch, Britisch`) wird nur die erste genommen.

### A.4 Sprachniveau

Entspricht `LANGUAGE_EXPORT_MAP` im Modell.

| `jd.<lang>_<skill>` | reading | writing | speaking | listening |
|---|---|---|---|---|
| `none` | `Not reading` | `Not writing` | `Not speaking` | `Not listening` ⚠️ |
| `basic` | `Basic` | `Basic` | `Basic` | `Basic` |
| `conversational` | `Conversational` | … | … | … |
| `proficient` | `Proficient` | … | … | … |
| `fluent` | `Fluent` | … | … | … |
| `native` | `Native` | … | … | … |

⚠️ Ausnahme: **English listening** (AJ) erwartet `Not Listening` mit großem L. Alle anderen listening-Spalten erwarten `Not listening`.

### A.5 T-Shirt

| `shirt_size` | Template |
|---|---|
| `XXS` (nächstmögliche), `XS` | `XS - (Chest 80-88cm)` |
| `S` | `S (Chest 88-96cm)` |
| `M` | `M (96-104cm)` |
| `L` | `L (104-112cm)` |
| `XL` | `XL (112-120cm)` |
| `XXL` | `XXL (120-128cm)` |
| `XXXL`, `XXXXL` (nächstmögliche) | `3XL (128-136cm)` |
| `Größe auswählen`, `NULL` | leer, Person in der Warnliste ausgeben |

### A.6 Reisedokument

| `jd.travel_document_type` | Template |
|---|---|
| `official_passport` (Hitobito-Label: Personalausweis) | `Other travel document (please specify)`; BX: `German ID card (Personalausweis)` |
| `ordinary_passport` | `Ordinary passport` |
| `diplomatic_passport` | `Diplomatic passport` |
| `service_passport` | `Service passport` |
| `special_passport` | `Special passport` |
| `other_travel_document` | `Other travel document (please specify)` |

### A.7 Visum

| `jd.visa_needed` | Template |
|---|---|
| `no` (Default) | `No, I do not need a visa` |
| `yes_have_one` | `Visa exists` |
| `yes_need_support` | `Yes, I do need a Schengen visa` |

### A.8 Ernährung

| `diet` | Template (EE) |
|---|---|
| `omnivorous` | `No special diet` |
| `vegetarian` | `Vegetarian` |
| `vegan` | `Vegan` |
| `halal` | `Halal` |
| `kosher` | `Kosher` |
| `omnivorous` und `jd.dietary_requirements` oder `jd.dietary_details` gesetzt | `Other` |
| – | `Hinduism - no beef` (keine Quelle in der DB) |

### A.9 Allergie-Schweregrad

| `jd.allergy_<x>_severity` | Template |
|---|---|
| `severe` | `Severe anaphylactic reaction (could result in death)` |
| `medium` | `Medium response & requires medical intervention` |
| `mild` | `Mild response that can be self managed` |

### A.10 Medizinische Geräte

| `jd.medical_equipment_needs[]` | Template (FZ) |
|---|---|
| `battery_charging_station` | `I need battery charging for medical equipment or aids (e.g., Continuous positive airway (CPAP) machine user)` |
| `continuous_power_supply` | `I need permanent electrical supply for medical equipment or aids` |
| `medication_cooling` | `I need refrigeration of medication` |
| `other` | `Other` |
| leer | leer |

Mehrere Werte werden in der Reihenfolge des Arrays übersetzt und mit `; ` verbunden. Beispiel: `[battery_charging_station, medication_cooling]` ergibt `I need battery charging … (CPAP) machine user); I need refrigeration of medication`.

### A.11 Ja/Nein

| DB | Template |
|---|---|
| `true` | `Yes` |
| `false` / Schlüssel fehlt | `No` |

---

## Anhang B – Offene Punkte

| # | Thema | Spalten | Frage | Antwort|
|---|---|---|---|---|
| 1 | Personenauswahl | – | Welche Status sollen exportiert werden: alle außer abgemeldet, oder nur `confirmed`? Werden JPT und EXT exportiert? | nur confirmed wird exportiert JPT und EXT werden auch wenn sie confirmed sind nicht exportiert |
| 2 | Datumsformat | M, BZ, CA | Ist `yyyy/mm/dd` korrekt? (Kopfzeile und Beschreibung widersprechen sich.) | ja korrekt |
| 3 | Patrol ID | E | Es gibt kein Feld. Woher kommt die Patrol ID (Dropdown für ULs)? | muss noch impleniert werden |
| 4 | Begleitperson (AC) | AB–AE | Wie heißt der Tag, und wie ist die AC-Person der YP zugeordnet? | ist so selten, der fall, dass das händisch nachgetragen wird wenn es fünf ULs gibt |
| 5 | Pre-selected IST | BF, BG | Über welche Gruppe oder welchen Tag werden Pavillon- und Food-House-IST erkannt? | Alle Personen die den Tag "Preselected-IST" haben oder in der Gruppe BMT sind  |
| 6 | Konstanten | C, D, AF, BQ–BV, DP–EB | Welche Werte gelten für den CMT-Notfallkontakt (Name, E-Mail, Telefon)? Native Language `German`? | Name: Notfalltelefon, E-mail: notfallkontakt@worldscoutjamboree.de, Telefon: TODO  |
| 7 | E-Mail | Z | Für YP nur bei Visum? Für IST und CMT die Jamboree-Adresse (`additional_info.wsjrdp_email`)? | immer ausfüllen mit der primary mail um später ein mapping zu haben  |
| 8 | Telefon | AA, BO | Welches Label hat Vorrang? Müssen die Nummern in ein internationales Format gebracht werden? | kontante Telefonnummer: TODO  |
| 9 | Name der Erziehungsberechtigten | BI, BJ | Ist die Aufteilung von `additional_contact_name_a` in Vor- und Nachname akzeptabel? | ja  |
| 10 | Nationalität | R | `jd.nationality` ist noch leer. Ist der Fallback `passport_nationality` mit Adjektiv-Mapping ok? | ja  |
| 11 | Medizin | FP–FX | Mit dem medizinischen Team klären: Freitexte sind deutsch; STIKO als Tetanus/Diphtherie werten? | felder aus der Datenbank nutzen  |
| 12 | Medizinische Geräte | FZ | Das Template erlaubt nur einen Wert, die DB speichert ein Array. Den ersten Wert nehmen und den Rest in GA schreiben? | werte mergen und ganzes array nehmen  |
| 13 | T-Shirt | BE | Wird `XXXXL` auf `3XL` gesetzt? Was passiert bei fehlender Größe? | nächstmögliche Größe |
| 14 | Komfort-Felder | AW–BC | Bedeutet ein fehlender Schlüssel „nicht wohl“? Oder nur werten, wenn `jamboree_data_confirmed = true` ist? | ein fehlender schlüssel soll als false interpretiert werden |
| 15 | Anreise / Abreise | DD, DG, DJ, DM | Diese Pflicht-Dropdowns sind noch unbekannt. Wie werden sie nachgetragen? | muss noch implementiert werden  |
| 16 | Visa-Details | CG–CY | Die Daten werden nicht in der DB erfasst. Wie werden sie für Personen mit `yes_need_support` beschafft? | werden händisch nachgetragen  |

### Noch offen

| Thema | Spalten | Was fehlt |
|---|---|---|
| `CONTINGENT_PHONE` | AA, BO, DV | feste Telefonnummer |
| Notfallkontakt Nachname | DQ | Wert für das Pflichtfeld |
| Native Language | AF | Ist `German` richtig? |
| Anreise / Abreise | DD, DG, DJ, DM | Felder in hitobito |
| `wsj_role = NULL` | A | Rolle aus `payment_role` ableiten oder Person nicht exportieren? |
| Head of Contingent | A | Welcher Tag (`HoC`?) |
| Travel Doc Other | BX | Text bei `other_travel_document` |

## Anhang C – Hinweise für das Export-Skript

- Personen laden mit `wsjrdp2027.load_people_dataframe(...)` bzw. eigenem SQL über `ctx.hitobito_psycopg_connection(read_only=True)`. Benötigte Zusatzspalten: `wsj_role`, `jamboree_data`, `additional_contact_*_a`, `passport_nationality`, `shirt_size`, `diet`, `medical_*`, Tags, Telefonnummern.
- Das Template mit **openpyxl** öffnen und befüllen, nicht neu erzeugen. So bleiben die Dropdowns und die versteckten Blätter erhalten. Spalten über den Namen in Zeile 2 ansprechen, nicht hart über Buchstaben.
- Jeden Dropdown-Wert gegen das zugehörige versteckte Blatt validieren. Bei unbekannten Werten die Zelle leer lassen und eine Warnung ausgeben. Ausnahme ist FZ mit mehreren Werten.
- Am Ende eine Warnliste ausgeben: unbekannte Länder- und Nationalitätswerte, fehlende T-Shirt-Größen und Personen mit `visa_needed = yes_need_support` (Visa-Details von Hand nachtragen).
- Ausgabe über `ctx.make_out_path(...)` nach `data/<script>/`.
- Die Daten sind personenbezogen und medizinisch. Das Ergebnis nicht committen.

## Anhang D – Testfälle (Personen 2 und 2334)

Erwartete Werte für ausgewählte Spalten. Namen, Dokumentnummern und andere Freitexte sind hier bewusst weggelassen.

| Sp. | Feld | id 2 (CMT) | id 2334 (IST) |
|---|---|---|---|
| A | Sub Category | `Contingent Management Team` | `International Service Team` |
| B | GDPR | `Yes` | `Yes` |
| I | Known as name | Fallback `nickname` → `first_name` | Fallback `nickname` → `first_name` |
| O | Country of birth | `Deutschland` → `Germany` | `Deutschland` → `Germany` |
| P | Gender | `Male` | `Female` |
| R | Nationality | `Deutsch` → `Germany` | `Deutsch` → `Germany` |
| AG–AJ | English r/w/s/l | `Fluent` / `Proficient` / `Fluent` / `Fluent` | `Conversational` / `Not writing` / `Proficient` / `Not Listening` |
| AK–AN | French r/w/s/l | `Not reading` / `Not writing` / `Not speaking` / `Not listening` | `Proficient` / `Not writing` / `Basic` / `Proficient` |
| AO–AR | Spanish r/w/s/l | `Not reading` / `Not writing` / `Basic` / `Basic` | `Conversational` / `Conversational` / `Conversational` / `Proficient` |
| AS–AV | Arabic r/w/s/l | `Not reading` / `Not writing` / `Not speaking` / `Not listening` | `Not reading` / `Basic` / `Not speaking` / `Not listening` |
| AW | on Water | `I do feel comfortable` | `I do not feel comfortable` |
| AX | in Water | `I do feel comfortable` | `I do feel comfortable` |
| AY | without balance | `I do feel comfortable` | `I do not feel comfortable` |
| AZ | large crowd | `I do not feel comfortable` | `I do not feel comfortable` |
| BA | flashing lights | `I do feel comfortable` | `I do feel comfortable` |
| BB | strong noise | `I do not feel comfortable` | `I do not feel comfortable` |
| BC | confined space | `I do not feel comfortable` | `I do not feel comfortable` |
| BE | T-Shirt | `M (96-104cm)` | `S (Chest 88-96cm)` |
| BF / BG | Pre-selected IST | leer (kein IST) | `No` / `No` (kein Tag, Gruppe 52) |
| BI–BV | Erziehungsberechtigte | leer (kein YP) | leer (kein YP) |
| BW | Travel Document Type | `Ordinary passport` | `Ordinary passport` |
| BZ / CA | Issue / Expiration | `2024/09/06` / `2032/09/17` | `2012/09/01` / `2029/09/15` |
| CB | Issued By | `Deutschland` → `Germany` | `Malaysia` → `Malaysia` |
| CC | Visa | `No, I do not need a visa` | `Yes, I do need a Schengen visa` |
| CE / CF | Place / Country of Birth (Visa) | leer | befüllt (`Germany`); Person steht in der Warnliste für Visa-Details |
| ED | Dietary requirements? | `Yes` (Freitext gesetzt) | `Yes` (`vegetarian`) |
| EE | Special diet II | `Other` | `Vegetarian` |
| EF | Special diet III | `dietary_requirements; dietary_details` | `dietary_requirements` |
| EG | Any allergies? | `Yes` | `Yes` |
| EH / EI | Milk | `Yes` / `Medium response …` | `Yes` / `Medium response …` |
| EJ / EK | Casein | `Yes` / `Severe anaphylactic …` | `No` / leer |
| EL / EM | Egg | `Yes` / `Mild response …` | `No` / leer |
| EN / EO | Peanuts | `No` / leer | `Yes` / `Mild response …` |
| FD / FE | Molluscs | `Yes` / leer (kein Schweregrad erfasst) | `No` / leer |
| FN / FO | Shellfish (`seafood`) | `No` / leer | `Yes` / `Severe anaphylactic …` |
| FP | Chronic diseases? | `No` | `No` |
| FS | Refrigerated medication | `Yes` | `Yes` |
| FU / FW | Tetanus / Diphteria | `Yes` / `Yes` | `Yes` / `Yes` |
| FZ | Medical equipment | `I need battery charging …; I need refrigeration of medication` | `Other` |
| GA | Equipment details | leer | befüllt |
| GB | Mobility restrictions | `Yes` | `Yes` |
| GC / GD | Wheelchair / Type | `Yes` / `Manual` | `No` / leer |
| GE | Cane/Stick/Frame | `No` | `Yes` |
| GF / GG | Other mobility needs | `Yes` / befüllt | `No` / leer |
| GI, GM, GN | Non-visible / sensory | `No`, `No`, `No` | `No`, `No`, `No` |

Auffälligkeiten in den Testdaten:

- id 2: Bei Molluscs fehlt der Schweregrad (`allergy_molluscs` ist gesetzt, `allergy_molluscs_severity` nicht).
- id 2: `diet = omnivorous`, aber im Freitext steht `Vegan`.
- id 2334: `visa_needed = yes_need_support`, obwohl die Nationalität `Deutsch` ist. Das Dokument ist in `Malaysia` ausgestellt.
