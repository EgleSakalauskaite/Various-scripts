WITH
auth AS (
	SELECT *
	FROM {{ ref("FactWebAuthentications") }}
	WHERE authentication_order_by_anonymous = 1
),
sessions AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY anonymous_id ORDER BY session_start_time, session_id) AS anonymous_step,
		ROW_NUMBER() OVER (PARTITION BY first_user_by_anonymous ORDER BY session_start_time, anonymous_id) AS user_step
	FROM {{ ref("FactWebSessions") }}
	WHERE anonymous_id IS NOT NULL
),
clients AS (
	SELECT
		korys_client_id,
		korys_contact_created_date AT TIME ZONE 'UTC' AS korys_contact_created_date
	FROM  {{ ref("DimClients") }}
	WHERE korys_contact_created_date IS NOT NULL
),
all_orders AS (
	SELECT
		o.anonymous_id,
        order_type,
        original_timestamp AT TIME ZONE 'UTC' AS original_timestamp
	FROM {{ ref("FactWebOrders") }} o
	WHERE anonymous_id IS NOT NULL
),
do_orders AS (
	SELECT
		anonymous_id,
        min(original_timestamp) AS first_order_attempt
	FROM all_orders o
	WHERE order_type IN (
		'order_complete',
		'choose_payment',
		'proceed_to_merchant',
		'add_to_cart',
		'proceed_to_cart'
	)
	GROUP BY 1
),
think_orders AS (
	SELECT
		anonymous_id,
        min(original_timestamp) AS first_order_page_visit
	FROM all_orders o
	WHERE order_type NOT IN (
		'order_complete',
		'choose_payment',
		'proceed_to_merchant',
		'add_to_cart',
		'proceed_to_cart'
	)
	GROUP BY 1
),
all_forms AS (
	SELECT
        anonymous_id,
		form_name,
        original_timestamp AT TIME ZONE 'UTC' AS original_timestamp
	FROM {{ ref("FactWebForms") }} f
	WHERE anonymous_id IS NOT NULL
),
do_forms AS (
	SELECT
		anonymous_id,
        min(original_timestamp) AS first_do_form
	FROM all_forms f
	WHERE form_name IN ('custom_server', 'gpu_pre_order', 'test_server')
	GROUP BY 1
),
think_forms AS (
	SELECT
		anonymous_id,
        min(original_timestamp) AS first_think_form
	FROM all_forms f
	WHERE form_name ~ 'waiting|interest|consultation|contact'
	GROUP BY 1
),
aLL_sales AS (
	SELECT
        korys_client_id,
        order_state,
		least(resource_date_start, resource_value_date) AS resource_date_start
	FROM {{ ref("FactSales") }} s
	WHERE korys_client_id IS NOT NULL 
),
do_sales AS (
	SELECT
        korys_client_id,
        min(resource_date_start) AS first_new_order
	FROM all_sales
	WHERE order_state = 'new_new'
	GROUP BY 1
),
care_sales AS (
	SELECT
        korys_client_id,
        min(resource_date_start) AS first_old_order
	FROM all_sales
	WHERE order_state IN ('new_old', 'renewal')
	GROUP BY 1
),
resources AS (
	SELECT
		resource_id,
		partner_id,
		date_start,
		least(date_cancel, resource_value_date) AS date_end
	FROM {{ ref("FactResources") }}
),
churns AS (
	SELECT
		team_owner_client_id AS korys_client_id,
		max(date_end  + INTERVAL '6 months') AS churn_date
	FROM resources r
	JOIN {{ ref("DimTeams") }} t
		ON r.partner_id = t.odoo_partner_id
	GROUP BY 1
),
see_conversions AS (
-- first website visit
	SELECT
		anonymous_id,
		first_user_by_anonymous AS korys_client_id,
		attribution_id,
		session_start_time AT TIME ZONE 'UTC' AS see_entered_at
	FROM sessions
	WHERE anonymous_step = 1
		AND (first_user_by_anonymous IS NULL OR user_step = 1)
),
think_conversions AS (
--	contact creation, visiting product order page or filling contact/waiting list forms
--  could be expanded with hubspot interactions data
	SELECT
        sc.anonymous_id,
		COALESCE(c.korys_client_id, sc.korys_client_id) AS korys_client_id,
		attribution_id,
		sc.see_entered_at, 
        least(c.korys_contact_created_date, o.first_order_page_visit, f.first_think_form) AS think_entered_at
	FROM see_conversions sc
	FULL JOIN clients c 
		ON sc.korys_client_id = c.korys_client_id
	LEFT JOIN think_orders o
	    ON sc.anonymous_id = o.anonymous_id
	LEFT JOIN think_forms f
	    ON sc.anonymous_id = f.anonymous_id
),
do_conversions AS (
--	first order (attempt) or signing up for pre-order/test server
--  could be expanded with hubspot interactions data
	SELECT
        tc.*,
        least(first_new_order, first_order_attempt, first_do_form) AS do_entered_at
	FROM think_conversions tc 
	LEFT JOIN do_sales s
	    ON tc.korys_client_id = s.korys_client_id
	LEFT JOIN do_orders o
	    ON tc.anonymous_id = o.anonymous_id
	LEFT JOIN do_forms f
	    ON tc.anonymous_id = f.anonymous_id
),
care_conversions AS (
--	first renewal or new_old order
--  could add case study completed and Trustpilot review obtained
	SELECT
		dc.*,
		first_old_order AS care_entered_at
	FROM do_conversions dc
	LEFT JOIN care_sales os
		ON dc.korys_client_id = os.korys_client_id
),
model AS (
	SELECT
		MD5(CONCAT_WS('|',
			COALESCE(anonymous_id, 'NULL'),
			COALESCE(cc.korys_client_id, 'NULL')
		)) AS lead_id,
        anonymous_id,
		cc.korys_client_id,
		attribution_id,
		LEAST(see_entered_at, think_entered_at, do_entered_at, care_entered_at) AS see_entered_at,
        LEAST(think_entered_at, do_entered_at, care_entered_at) AS think_entered_at,
        LEAST(do_entered_at, care_entered_at) AS do_entered_at,
		care_entered_at,
		CASE
			WHEN care_entered_at IS NULL THEN NULL
			ELSE churn_date
		END AS care_exited_at
    FROM care_conversions cc
	LEFT JOIN churns c
		ON cc.korys_client_id = c.korys_client_id
)
SELECT *
FROM model