WITH 
ltv AS (
	SELECT
		date_trunc('month', dt.first_order_date) AS month,
		SUM(CASE WHEN medium ~ 'cpc|ppc' THEN fs2.amount ELSE 0 END) AS gads_ltv,
		SUM(CASE WHEN da.default_channel_grouping = 'Affiliate' THEN fs2.amount ELSE 0 END) AS affiliate_ltv
	FROM "FactSales" fs2 
	JOIN "DimTeams" dt 
		ON dt.odoo_partner_id = fs2.partner_id
	JOIN "DimAttribution" da 
		ON fs2.attribution_id = da.attribution_id
	WHERE fs2.invoice_state IN ('paid', 'confirm', 'invoice')
	GROUP BY 1
),
ad_cost AS (
	SELECT
		date_trunc('month', date) AS month,
		sum(cost) AS total_ad_costs
	FROM "FactAdQuality" faq 
	GROUP BY 1
),
affiliates_cost AS (
	SELECT 
		date_trunc('month', date_invoice) AS month,
		sum(commission_amt) AS total_affiliate_costs
	FROM "FactAffiliateVisits" fav
	WHERE state IN ('paid', 'confirm', 'invoice')
	GROUP BY 1
),
monthly AS (
	SELECT
		l.month AS month,
		COALESCE(l.gads_ltv, 0) AS gads_ltv,
		COALESCE(ad.total_ad_costs, 0) AS total_gads_costs,
		COALESCE(l.affiliate_ltv, 0) AS affiliate_ltv,
		COALESCE(af.total_affiliate_costs, 0) AS total_affiliate_costs
	FROM ltv l
	LEFT JOIN ad_cost ad
		ON l.month = ad.month
	LEFT JOIN affiliates_cost af
		ON l.month = af.month
	ORDER BY month
),
model AS (
	SELECT 
		*,
		CASE 
	        WHEN total_gads_costs > 0 THEN (gads_ltv - total_gads_costs) / total_gads_costs 
	        ELSE NULL 
	    END AS gads_ROI,
	    CASE 
	        WHEN total_affiliate_costs > 0 THEN (affiliate_ltv - total_affiliate_costs) / total_affiliate_costs
	        ELSE NULL 
	    END AS affiliate_ROI
	FROM monthly
	WHERE MONTH >= '2024-01-01'
)
SELECT *
FROM model
