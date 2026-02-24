-- Update wsjrdp_direct_debit_pre_notifications and accounting_entries
-- to have proper direct_debit_payment_info_id
UPDATE wsjrdp_direct_debit_pre_notifications SET direct_debit_payment_info_id = pmntinf.id
FROM wsjrdp_direct_debit_payment_infos pmntinf
WHERE wsjrdp_direct_debit_pre_notifications.direct_debit_payment_info_id IS NULL
  AND wsjrdp_direct_debit_pre_notifications.payment_status = 'xml_generated'
  AND wsjrdp_direct_debit_pre_notifications.payment_initiation_id = pmntinf.payment_initiation_id
  AND wsjrdp_direct_debit_pre_notifications.debit_sequence_type = pmntinf.debit_sequence_type;

UPDATE accounting_entries SET direct_debit_payment_info_id = pn.direct_debit_payment_info_id
FROM wsjrdp_direct_debit_pre_notifications pn
WHERE accounting_entries.direct_debit_payment_info_id IS NULL
  AND accounting_entries.direct_debit_pre_notification_id = pn.id
  AND pn.direct_debit_payment_info_id IS NOT NULL;

-- Add new payment initiation
INSERT INTO wsjrdp_payment_initiations (id, created_at, status, sepa_schema, message_identification, number_of_transactions, control_sum_cents, initiating_party_name, initiating_party_iban, initiating_party_bic)
SELECT 6, '2026-02-09 09:12:10.662203', 'xml_generated', 'pain.008.001.02', '20260205115914-36344dc7520c', 1073, 50227443, 'Ring deutscher Pfadfinder*innenverbaende e.V.', 'DE13370601932001939044', 'GENODED1PAX'
WHERE NOT EXISTS (SELECT id FROM wsjrdp_payment_initiations WHERE id = 6);

-- Fix payment initiation 5 (2026-02-05 debit)
UPDATE wsjrdp_payment_initiations SET number_of_transactions = 6, control_sum_cents = 1520250 WHERE id = 5 AND number_of_transactions <> 6;
-- Fix payment info 6 (2026-02-09 debit)
UPDATE wsjrdp_direct_debit_payment_infos SET number_of_transactions = 1073, control_sum_cents = 50227443, requested_collection_date='2026-02-09', payment_initiation_id = 6 WHERE id = 6 AND number_of_transactions <> 1073;

-- Fix value_date / collection_date
UPDATE accounting_entries SET value_date = '2026-02-09' WHERE direct_debit_payment_info_id = 6 AND value_date = '2026-02-05';
UPDATE wsjrdp_direct_debit_pre_notifications SET collection_date = '2026-02-09' WHERE direct_debit_payment_info_id = 6 AND collection_date = '2026-02-05';

-- Fix payment_initiation_id
UPDATE accounting_entries SET payment_initiation_id = 6 WHERE direct_debit_payment_info_id = 6 AND payment_initiation_id = 5;
UPDATE wsjrdp_direct_debit_pre_notifications SET payment_initiation_id = 6 WHERE direct_debit_payment_info_id = 6 AND payment_initiation_id = 5;

-- Fix endtoend_id
UPDATE accounting_entries SET endtoend_id = endtoend_id || '-2' WHERE direct_debit_payment_info_id = 6 AND right(endtoend_id, 2) <> '-2';
UPDATE wsjrdp_direct_debit_pre_notifications SET endtoend_id = endtoend_id || '-2' WHERE direct_debit_payment_info_id = 6 AND right(endtoend_id, 2) <> '-2';

-- SELECTs to check consistency
SELECT * FROM wsjrdp_payment_initiations;
SELECT * FROM wsjrdp_direct_debit_payment_infos;


SELECT COUNT(id) AS num, 'direct_debit_payment_info_id = 6 AND payment_initiation_id <> 6' AS description FROM accounting_entries WHERE direct_debit_payment_info_id = 6 AND payment_initiation_id <> 6
UNION
SELECT COUNT(id) AS num, 'accounting_entries endtoend_id ends not with "-2"' AS description FROM accounting_entries WHERE direct_debit_payment_info_id = 6 AND right(endtoend_id, 2) <> '-2'
UNION
SELECT COUNT(ae.id) AS num, 'inconsistent payment_initiation_id or direct_debit_payment_info_id between ae and corresponding pn' AS description FROM accounting_entries ae LEFT JOIN wsjrdp_direct_debit_pre_notifications pn ON ae.direct_debit_pre_notification_id = pn.id
WHERE ae.payment_initiation_id <> pn.payment_initiation_id OR ae.direct_debit_payment_info_id <> pn.direct_debit_payment_info_id
UNION
SELECT COUNT(ae.id) AS num, 'missing payment_initiation_id or direct_debit_payment_info_id on ae entries' AS description FROM accounting_entries ae LEFT JOIN wsjrdp_direct_debit_pre_notifications pn ON ae.direct_debit_pre_notification_id = pn.id
WHERE pn.id IS NOT NULL AND (ae.payment_initiation_id IS NULL or ae.direct_debit_payment_info_id IS NULL)
UNION
SELECT COUNT(ae.id) AS num, 'accounting_entries in pmnt info 6' FROM accounting_entries ae WHERE ae.direct_debit_payment_info_id = 6
UNION
SELECT COUNT(ae.id) AS num, 'accounting_entries in pmnt info 7' FROM accounting_entries ae WHERE ae.direct_debit_payment_info_id = 7
ORDER BY num;
