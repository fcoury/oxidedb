-- Close duplicate open session tabs per workspace, keeping the newest.
-- Safe to re-run; it only flips is_open for extra rows.

WITH ranked AS (
  SELECT
    id,
    workspace_id,
    ROW_NUMBER() OVER (
      PARTITION BY workspace_id
      ORDER BY datetime(created_at) DESC, id DESC
    ) AS rn
  FROM session_tabs
  WHERE is_open = 1
    AND workspace_id IS NOT NULL
)
UPDATE session_tabs
SET is_open = 0
WHERE id IN (SELECT id FROM ranked WHERE rn > 1);

-- Optional: normalize tab_index for open tabs after cleanup.
-- Uncomment if you want contiguous indices, preserving current order.
--
-- WITH ordered AS (
--   SELECT
--     id,
--     ROW_NUMBER() OVER (
--       ORDER BY tab_index ASC, datetime(created_at) ASC, id ASC
--     ) - 1 AS new_index
--   FROM session_tabs
--   WHERE is_open = 1
-- )
-- UPDATE session_tabs
-- SET tab_index = (SELECT new_index FROM ordered WHERE ordered.id = session_tabs.id)
-- WHERE id IN (SELECT id FROM ordered);
