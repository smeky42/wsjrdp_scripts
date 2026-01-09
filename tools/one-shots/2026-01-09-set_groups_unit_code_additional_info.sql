-- Update groups for units to have the additional_info attribute
-- 'unit_code' filled with the units unit_code.

WITH unit_code_map AS (
  SELECT g.id, g.name, p.unit_code, COUNT(p.id) FROM people p LEFT JOIN groups g ON p.primary_group_id = g.id
  WHERE g.name LIKE '__'
  GROUP BY g.id, g.name, p.unit_code
  ORDER BY g."name"
)
UPDATE groups SET additional_info['unit_code'] = to_jsonb(ucm.unit_code) FROM unit_code_map AS ucm WHERE ucm.id = groups.id

-- Check the result:
--
-- SELECT id, name, additional_info->>'unit_code' AS unit_code FROM groups;
--
--
-- Check for misplaced people:
--
-- SELECT p.id, p.first_name, p.last_name, p.unit_code, g.additional_info->>'unit_code' AS group_unit_code
-- FROM people p LEFT JOIN groups g ON p.primary_group_id = g.id
-- WHERE p.unit_code IS NOT NULL AND p.unit_code <> g.additional_info->>'unit_code';
