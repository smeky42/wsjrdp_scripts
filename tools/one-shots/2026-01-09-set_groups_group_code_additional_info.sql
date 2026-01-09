-- Update groups to have the additional_info attribute
-- 'group_code'. For units the 'group_code' is the letter+number
-- unit-id.

UPDATE groups SET additional_info['group_code'] = to_jsonb(name) WHERE name LIKE '__';
UPDATE groups SET additional_info['group_code'] = to_jsonb('CMT'::text) WHERE id = 1;
UPDATE groups SET additional_info['group_code'] = to_jsonb('UL'::text) WHERE id = 2;
UPDATE groups SET additional_info['group_code'] = to_jsonb('YP'::text) WHERE id = 3;
UPDATE groups SET additional_info['group_code'] = to_jsonb('IST'::text) WHERE id = 4;
UPDATE groups SET additional_info['group_code'] = to_jsonb('BMT'::text) WHERE id = 45;

-- Check the result:
-- SELECT id, name, additional_info->>'group_code' AS group_code FROM groups ORDER BY id;
