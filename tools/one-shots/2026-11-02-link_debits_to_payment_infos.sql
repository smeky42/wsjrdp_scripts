-- Links the collected pre-notifications (payment_status 'xml_generated') and
-- their accounting entries to the payment info (Sammler) they went into, where
-- the link is missing, and removes the link from skipped pre-notifications,
-- which went into no payment info. sepa_direct_debit.py sets the link since it
-- creates the payment infos before writing; older runs left it empty or set it
-- on skipped ones.
--
-- A pre-notification goes into the payment info of its payment initiation
-- with its sequence type and collection date -- the PmtInf of a PAIN message
-- is one per sequence type and date. Only an exact single match is used.
--
-- Idempotent: only empty links are filled and only set links of skipped
-- pre-notifications cleared; a second run changes nothing and passes the same
-- checks. updated_at is left alone (a data repair, no edit).
--
-- Run: psql -v ON_ERROR_STOP=1 -f <this file>

BEGIN;

-- Before: every unlinked collected pre-notification has exactly one
-- candidate payment info.
DO $$
DECLARE
  bad integer;
BEGIN
  SELECT count(*) INTO bad
  FROM wsjrdp_direct_debit_pre_notifications pn
  WHERE pn.payment_status = 'xml_generated'
    AND pn.direct_debit_payment_info_id IS NULL
    AND (SELECT count(*) FROM wsjrdp_direct_debit_payment_infos i
         WHERE i.payment_initiation_id = pn.payment_initiation_id
           AND i.debit_sequence_type = pn.debit_sequence_type
           AND i.requested_collection_date = pn.collection_date) <> 1;
  IF bad > 0 THEN
    RAISE EXCEPTION '% collected pre-notifications have no or several candidate payment infos', bad;
  END IF;
END $$;

WITH matched AS (
  SELECT pn.id, i.id AS payment_info_id
  FROM wsjrdp_direct_debit_pre_notifications pn
  JOIN wsjrdp_direct_debit_payment_infos i
    ON i.payment_initiation_id = pn.payment_initiation_id
   AND i.debit_sequence_type = pn.debit_sequence_type
   AND i.requested_collection_date = pn.collection_date
  WHERE pn.payment_status = 'xml_generated'
    AND pn.direct_debit_payment_info_id IS NULL
)
UPDATE wsjrdp_direct_debit_pre_notifications pn
SET direct_debit_payment_info_id = matched.payment_info_id
FROM matched
WHERE pn.id = matched.id;

UPDATE wsjrdp_direct_debit_pre_notifications
SET direct_debit_payment_info_id = NULL
WHERE payment_status = 'skipped'
  AND direct_debit_payment_info_id IS NOT NULL;

UPDATE accounting_entries e
SET direct_debit_payment_info_id = pn.direct_debit_payment_info_id
FROM wsjrdp_direct_debit_pre_notifications pn
WHERE pn.id = e.direct_debit_pre_notification_id
  AND pn.direct_debit_payment_info_id IS NOT NULL
  AND e.direct_debit_payment_info_id IS NULL;

-- After: nothing collected without a payment info, entries agree with their
-- pre-notification, and every payment info holds exactly the number and sum
-- of debits its PmtInf declared.
DO $$
DECLARE
  unlinked integer;
  skipped_linked integer;
  entry_diff integer;
  info_diff integer;
  linked_pns integer;
  linked_entries integer;
BEGIN
  SELECT count(*) INTO unlinked
  FROM wsjrdp_direct_debit_pre_notifications
  WHERE payment_status = 'xml_generated' AND direct_debit_payment_info_id IS NULL;
  IF unlinked > 0 THEN
    RAISE EXCEPTION '% collected pre-notifications still without payment info', unlinked;
  END IF;

  SELECT count(*) INTO skipped_linked
  FROM wsjrdp_direct_debit_pre_notifications
  WHERE payment_status = 'skipped' AND direct_debit_payment_info_id IS NOT NULL;
  IF skipped_linked > 0 THEN
    RAISE EXCEPTION '% skipped pre-notifications still linked to a payment info', skipped_linked;
  END IF;

  SELECT count(*) INTO entry_diff
  FROM accounting_entries e
  JOIN wsjrdp_direct_debit_pre_notifications pn ON pn.id = e.direct_debit_pre_notification_id
  WHERE pn.payment_status = 'xml_generated'
    AND e.direct_debit_payment_info_id IS DISTINCT FROM pn.direct_debit_payment_info_id;
  IF entry_diff > 0 THEN
    RAISE EXCEPTION '% accounting entries with another payment info than their pre-notification', entry_diff;
  END IF;

  SELECT count(*) INTO info_diff
  FROM wsjrdp_direct_debit_payment_infos i
  LEFT JOIN LATERAL (
    SELECT count(*) AS n, COALESCE(sum(pn.amount_cents), 0) AS cents
    FROM wsjrdp_direct_debit_pre_notifications pn
    WHERE pn.direct_debit_payment_info_id = i.id AND pn.payment_status = 'xml_generated'
  ) linked ON TRUE
  WHERE linked.n <> i.number_of_transactions OR linked.cents <> i.control_sum_cents;
  IF info_diff > 0 THEN
    RAISE EXCEPTION '% payment infos whose linked debits differ from their count or control sum', info_diff;
  END IF;

  SELECT count(*) INTO linked_pns
  FROM wsjrdp_direct_debit_pre_notifications WHERE direct_debit_payment_info_id IS NOT NULL;
  SELECT count(*) INTO linked_entries
  FROM accounting_entries WHERE direct_debit_payment_info_id IS NOT NULL;
  RAISE NOTICE 'OK: % pre-notifications and % accounting entries linked to a payment info', linked_pns, linked_entries;
END $$;

COMMIT;
