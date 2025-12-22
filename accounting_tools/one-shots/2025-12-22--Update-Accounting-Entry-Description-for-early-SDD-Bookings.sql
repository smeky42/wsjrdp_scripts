-- SQL script to overwrite the accounting_entries description with
-- the one from wsjrdp_direct_debit_pre_notifications for
-- payment_initiation_id IN (1, 2, 3)
UPDATE accounting_entries
SET description = pn.description
FROM wsjrdp_direct_debit_pre_notifications AS pn
WHERE pn.id = accounting_entries.direct_debit_pre_notification_id
  AND accounting_entries.direct_debit_pre_notification_id IS NOT NULL
  AND accounting_entries.payment_initiation_id IN (1, 2, 3);
