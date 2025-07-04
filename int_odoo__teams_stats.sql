WITH
teams AS (
        SELECT *
        FROM "alexandria"."dev_staging"."base_odoo__teams"
),
sales AS (
        SELECT *
        FROM "alexandria"."dev_staging"."base_odoo__sales"
),
products AS (
        SELECT *
        FROM "alexandria"."dev_staging"."base_odoo__products"
),
sales_activity AS (
    SELECT
        t.odoo_partner_id AS partner_id,
        t.first_order_date,
        MAX(s.resource_value_date) AS last_order_date,
                (EXTRACT(YEAR FROM MAX(s.resource_value_date)) - EXTRACT(YEAR FROM t.first_order_date)) * 12 +
            EXTRACT(MONTH FROM MAX(s.resource_value_date)) - EXTRACT(MONTH FROM t.first_order_date) AS active_months
         FROM teams t
         LEFT JOIN sales s
            ON t.odoo_partner_id = s.partner_id
         GROUP BY 1, 2
),
monthly_sales AS (
    SELECT 
        partner_id,
        ROUND(SUM(amount)) monthly_value
    FROM sales
    WHERE resource_state = 'active'
    AND date_trunc('month', NOW()) = date_trunc('month', resource_value_date)
    GROUP BY 1
),
product_categories AS (
    SELECT
        id AS product_id,
        CASE
            WHEN product_category_name ~ 'VPS' THEN 'vps'
            WHEN product_category_name ~ 'VDS' THEN 'vds'
            WHEN product_category_name ~ 'Outlet|weight' THEN 'dedicated'
            ELSE 'other'
        END AS category
    FROM products
),
active_resources AS (
    SELECT DISTINCT
        s.partner_id,
        s.product_id,
        s.resource_id,
        pc.category
    FROM sales s
    LEFT JOIN product_categories pc
        ON s.product_id = pc.product_id
    WHERE resource_state = 'active'
),
resource_counts AS (
    SELECT
        partner_id,
        SUM(CASE WHEN category = 'dedicated' THEN 1 ELSE 0 END) AS dedicated_active,
        SUM(CASE WHEN category = 'vds' THEN 1 ELSE 0 END) AS vds_active,
        SUM(CASE WHEN category = 'vps' THEN 1 ELSE 0 END) AS vps_active,
        SUM(CASE WHEN category = 'other' THEN 1 ELSE 0 END) AS other_active
    FROM active_resources
    GROUP BY 1
),
model AS (
    SELECT
        sa.partner_id,
        sa.first_order_date,
        sa.last_order_date,
        GREATEST(sa.active_months, 0) AS active_months,
        ms.monthly_value,
        rc.dedicated_active,
        rc.vds_active,
        rc.vps_active,
        rc.other_active,
        CASE
            WHEN rc.dedicated_active + rc.vds_active + rc.vps_active + rc.other_active > 0 THEN 1
        ELSE 0
        END AS is_resource_active
    FROM sales_activity sa
    LEFT JOIN monthly_sales ms
        ON sa.partner_id = ms.partner_id
    LEFT JOIN resource_counts rc
        ON sa.partner_id = rc.partner_id
)
SELECT *
FROM model