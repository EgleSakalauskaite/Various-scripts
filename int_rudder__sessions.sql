WITH
gads AS (
    SELECT DISTINCT
        gclid,
        ad_group_id,
        keyword_info_text
    FROM {{ ref("stg_gads__clickviews")}}
),
clients AS (
	SELECT *
	FROM {{ ref("base_odoo__clients")}}
),
identifies AS (
	SELECT *
	FROM {{ ref("stg_rudder__identifies")}}
	WHERE user_id IS NOT NULL
),
identifies_ordered AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY anonymous_id ORDER BY original_timestamp) AS authentication_order_by_anonymous,
    ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY original_timestamp) AS authentication_order_of_session
	FROM identifies
),
pageviews as (
    SELECT *
    FROM {{ ref("int_rudder__pages")}}
),
interactions AS (
	SELECT i.*,
	CONCAT(cast(split_part(split_part(replace(replace(replace(
        i.page_url,'android-app://',''),
        'http://',''),
        'https://',''),
        '/',1),
        '?',1)
 		as TEXT),
 		i.page_path)
 		AS page_full_path,
	ROW_NUMBER() OVER (PARTITION BY i.session_id ORDER BY i.original_timestamp) session_step
	FROM {{ ref("stg_rudder__interactions")}} i
	LEFT JOIN pageviews p
		ON i.session_id = p.session_id
	WHERE p.session_id IS NULL
),
pageviews_data AS (
   	SELECT
        session_id,
        COUNT(*) AS pageviews,
        MIN(original_timestamp) AS session_start_time,
        MAX(original_timestamp) AS session_end_time
    FROM pageviews
    GROUP BY 1
),
interactions_data AS (
	SELECT
	    session_id,
	    COUNT(DISTINCT page_url) AS pageviews,
	    MIN(original_timestamp) AS session_start_time,
	    MAX(original_timestamp) AS session_end_time
	FROM interactions
	GROUP BY 1
),
anonymous_identifies AS (
	SELECT anonymous_id, user_id
	FROM identifies_ordered
	WHERE anonymous_id IS NOT NULL
	AND authentication_order_by_anonymous = 1
),
session_identifies AS (
	SELECT session_id, user_id
	FROM identifies_ordered
	WHERE session_id IS NOT NULL
	AND authentication_order_of_session = 1
),
sessions AS (
    SELECT
        p.anonymous_id,
		p.user_id,
        p.session_id,
        pd.pageviews,
    	pd.session_start_time,
    	pd.session_end_time,
        p.campaign_name AS campaign,
        p.campaign_source AS source,
        p.campaign_medium AS medium,
        p.campaign_content AS content,
        p.campaign_term AS term,
        p.page_initial_referrer_domain AS referrer_domain,
        p.gclid,
        p.affiliate_code,
        p.page_full_path AS first_page_visited,
		p.user_agent
    FROM pageviews p
    JOIN pageviews_data pd
    	ON p.session_id = pd.session_id
    WHERE p.session_step = 1
    UNION ALL -- because OF filtering IN interactions, there IS certainly NO duplicate session_id, therefore UNION ALL IS chosen FOR faster performance
    SELECT
    	i.anonymous_id,
		i.user_id,
    	i.session_id,
    	id.pageviews,
    	id.session_start_time,
    	id.session_end_time,
    	i.campaign_name AS campaign,
    	i.campaign_source AS source,
    	i.campaign_medium AS medium,
    	i.campaign_content AS content,
    	i.campaign_term AS term,
    	i.page_initial_referrer_domain AS referrer_domain,
    	i.gclid,
    	NULL AS affiliate_code,
    	i.page_full_path AS first_page_visited,
		i.user_agent
    FROM interactions i
    JOIN interactions_data id
    	ON i.session_id = id.session_id
    WHERE i.session_step = 1
),
cherry_anonymus AS (
	SELECT DISTINCT s.anonymous_id
	FROM sessions s
	LEFT JOIN clients c
	ON s.user_id = c.korys_client_id
	WHERE s.referrer_domain ~ 'korys.cherryservers.com'
	OR c.is_company_partner IS TRUE 
),
tagged_sessions AS (
    SELECT
		s.*,
		ai.user_id AS first_user_by_anonymous,
		si.user_id AS first_user_by_session,
		CASE
			WHEN ca.anonymous_id IS NULL THEN FALSE
			ELSE TRUE
		END AS is_cherry,
		CASE
			WHEN s.user_agent ~ 'Windows NT|X11|Macintosh' THEN 'Desktop'
			WHEN lower(s.user_agent) ~ 'android|phone|ipad|mobile' THEN 'Mobile'
			WHEN lower(s.user_agent) ~ 'compatible|crawler|bot|amazon-qbusiness' THEN 'Bot'
			ELSE NULL
		END AS device,
	    CASE
	        WHEN s.medium ~ 'cpc|ppc' AND s.campaign ~ '^\[S\].+'
	            THEN 'Paid Search' 
	        WHEN s.medium ~ 'cpc|ppc' AND s.campaign ~ '^\[D(G)?\].+'
	            THEN 'Paid Display'
	        WHEN s.medium ~ 'cpc|ppc' AND s.campaign ~ '^\[V\].+'
	            THEN 'Paid Video'
	        WHEN (s.medium ~ 'social|linkedin') OR (s.medium IS NOT NULL AND s.source ~ 'linkedin|facebook|social media|twitter')
	            THEN 'Paid Social'
	        WHEN s.medium ~ 'email|newsletter' OR s.source ~ 'email|newsletter'
	            THEN 'Email'
			WHEN s.medium ~ 'cpc|ppc'
				THEN 'Paid Other'
	        WHEN s.campaign IS NULL AND s.medium IS NULL AND s.content IS NULL AND s.term IS NULL AND s.affiliate_code IS NULL
				AND (s.source ~ 'openai|chatgpt|gpt|gemini|bard|copilot|perplexity|neeva|writesonic|copy\.ai|astastic|outrider|nimble|bnngpt|edgeservices|claude|grok' OR referrer_domain ~'(^|[./])(openai|chatgpt|gpt|gemini|bard|copilot|perplexity|neeva|writesonic|copy\.ai|astastic|outrider|nimble|bnngpt|edgeservices|claude|grok)([./]|$)')
	            THEN 'AI'
	        WHEN s.campaign IS NULL AND s.source IS NULL AND s.medium IS NULL AND s.content IS NULL AND s.term IS NULL AND s.gclid IS NULL AND s.affiliate_code IS NULL 
	        	AND s.referrer_domain ~ '(^|[./])(syndicatedsearch.goog|google|bing|yandex|duckduckgo|search\.brave\.com|startpage\.com|yahoo|presearch.com|baidu.com|ya.ru|ecosia|qwant|coccoc|kagi.com)([./]|$)' 
	            THEN 'Organic Search'
	        WHEN s.campaign IS NULL AND s.source IS NULL AND s.medium IS NULL AND s.content IS NULL AND s.term IS NULL AND s.gclid IS NULL AND s.affiliate_code IS NULL
	        	AND s.referrer_domain ~ '(^|[./])(facebook|linkedin|twitter|t.co|instagram|telegram|skype|reddit)([./]|$)'
	            THEN 'Organic Social'
	        WHEN s.campaign IS NULL AND s.source IS NULL AND s.medium IS NULL AND s.content IS NULL AND s.term IS NULL AND s.gclid IS NULL AND s.affiliate_code IS NULL
	        	AND s.referrer_domain ~ '(^|[./])(youtube|tiktok)([./]|$)' 
	            THEN 'Organic Video'
	        WHEN s.affiliate_code IS NOT NULL
	            THEN 'Affiliate'
			WHEN (s.campaign IS NULL AND s.source IS NULL AND s.medium IS NULL AND s.content IS NULL AND s.term IS NULL AND s.gclid IS NULL AND s.affiliate_code IS NULL)
				AND (s.referrer_domain IS NULL OR s.referrer_domain ~ '^(acs|acs2|3ds)([./-]|$)' OR s.referrer_domain ~ '(^|[./])(accounts.google|sandbox.bvnk.com|stripe.com|translate.goog|cherryservers.com|coingate|paypal|paysera|idenfy|3dsecure|cardinalcommerce|wibmo|proxify|shareasale-analytics|rsa3dsauth)([./]|$)')
	            THEN 'Direct'
	        WHEN s.campaign IS NULL AND s.source IS NULL AND s.medium IS NULL AND s.content IS NULL AND s.term IS NULL AND s.gclid IS NULL AND s.affiliate_code IS NULL AND s.referrer_domain IS NOT NULL
	            THEN 'Referral'
	        ELSE 'Not set'
	    END AS default_channel_grouping,
	    ROW_NUMBER() OVER(PARTITION BY s.anonymous_id ORDER BY session_start_time, s.session_id) AS session_by_anonymous
    FROM sessions s
	LEFT JOIN anonymous_identifies ai
		ON s.anonymous_id = ai.anonymous_id
	LEFT JOIN session_identifies si
		ON s.session_id = si.session_id
	LEFT JOIN cherry_anonymus ca
		ON s.anonymous_id = ca.anonymous_id
),
model AS (
    SELECT
        s.anonymous_id,
        s.session_id,
		s.first_user_by_anonymous,
        s.first_user_by_session,
		MD5(CONCAT_WS('|',
			COALESCE(ga.ad_group_id, -1),
			COALESCE(s.campaign, 'NULL'),
			COALESCE(s.source, 'NULL'),
			COALESCE(s.medium, 'NULL'),
			COALESCE(s.content, 'NULL'),
			COALESCE(s.term, 'NULL'),
			COALESCE(ga.keyword_info_text, 'NULL'),
			COALESCE(s.referrer_domain, 'NULL'),
			COALESCE(s.affiliate_code, 'NULL'),
			COALESCE(s.default_channel_grouping, 'NULL')
		)) AS attribution_id,
        s.pageviews,
        s.session_start_time,
        s.session_end_time,
		s.device,
		s.is_cherry,
        s.default_channel_grouping,
		ga.ad_group_id,
        s.campaign,
        s.source,
        s.medium,
        s.content,
		ga.keyword_info_text AS keyword,
        s.term,
        s.referrer_domain,
        s.gclid,
        s.affiliate_code,
        s.first_page_visited,
        s.session_by_anonymous
    FROM tagged_sessions s
	LEFT JOIN gads ga
		ON s.gclid = ga.gclid
)
SELECT *
FROM model