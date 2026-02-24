-- Unit Managers
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('hannah.kiefer@worldscoutjamboree.de') WHERE (short_name LIKE 'A_');
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('bernhard.gruene@worldscoutjamboree.de') WHERE (short_name LIKE 'B_');
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('david.boehm@worldscoutjamboree.de') WHERE (short_name LIKE 'D_');
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('philipp.ranitzsch@worldscoutjamboree.de') WHERE (short_name LIKE 'E_');
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('kilian.schmidt@worldscoutjamboree.de') WHERE (short_name LIKE 'K_');

-- eHOC Unit Support
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('valentin.reichenspurner@worldscoutjamboree.de') WHERE (short_name LIKE '__') OR id IN (2, 3);

-- CMT Support
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('carl.espeter@worldscoutjamboree.de', 'kl@worldscoutjamboree.de') WHERE id = 1; -- CMT

-- IST Support
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('patrick.mayer@worldscoutjamboree.de') WHERE id IN (4, 45); -- IST-Support
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('martin.griwatz@worldscoutjamboree.de') WHERE id IN (4, 45); -- eHOC IST / BMT

-- FIN
UPDATE groups SET additional_info['support_cmt_mail_addresses'] = COALESCE(additional_info['support_cmt_mail_addresses'], '[]'::jsonb) || jsonb_build_array('david.fritzsche@worldscoutjamboree.de') WHERE additional_info['support_cmt_mail_addresses'] IS NOT NULL; -- FIN

-- Deduplicate
UPDATE groups SET additional_info = jsonb_set(additional_info, '{support_cmt_mail_addresses}', (SELECT jsonb_agg(v ORDER BY ord) FROM (SELECT DISTINCT ON (v) v, ord FROM jsonb_array_elements(additional_info->'support_cmt_mail_addresses') WITH ORDINALITY AS t(v, ord) ORDER BY v, ord)));
