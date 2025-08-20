{{
  config(
    materialized='ephemeral'
  )
}}

SELECT DISTINCT unnest(t.product_category_ids) as category_id
FROM {{ ref('stg_teams') }} t
INNER JOIN {{ ref('stg_team_members') }} tm ON t.id = tm.team_id
WHERE tm.user_id = 6375  -- Current user ID
  AND t.status = 1
  AND t.team_type = 0
  AND t.product_category_ids != '{}'
