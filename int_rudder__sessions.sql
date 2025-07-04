WITH
pageviews as (
    SELECT *
    FROM "alexandria"."dev_intermediate"."int_rudder__pages"
),
-- due to data collection errors, significant amount of sessions are only recorded in rudder_interactions and not in rudder_pages
interactions AS (
    SELECT i.*,
    CONCAT(cast(split_part(split_part(replace(replace(replace(
    		i.page_url,'android-app://',''),
    		'http://',''),
    		'https://',''),
    		'/',1),
    		'?',1)
        	AS TEXT),
        i.page_path)
    AS page_full_path,
    ROW_NUMBER() OVER (PARTITION BY i.session_id ORDER BY i.original_timestamp) session_step
    FROM "alexandria"."dev_staging"."stg_rudder__interactions" i
    LEFT JOIN pageviews p -- filter out sessions that already appear in pageviews TO avoid duplicate session_id
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
sessions AS (
    SELECT
        p.anonymous_id,
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
        p.page_full_path AS first_page_visited
    FROM pageviews p
    JOIN pageviews_data pd
        ON p.session_id = pd.session_id
    WHERE p.session_step = 1
    UNION ALL -- because OF filtering IN interactions, there IS certainly NO duplicate session_id, therefore UNION ALL IS chosen FOR faster performance
    SELECT
        i.anonymous_id,
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
        i.page_full_path AS first_page_visited
    FROM interactions i
    JOIN interactions_data id
        ON i.session_id = id.session_id
    WHERE i.session_step = 1
),
tagged_sessions AS (
    SELECT
        *,
        CASE
            WHEN medium ~ 'cpc|ppc' AND campaign ~ '^\[S\].+'
                THEN 'Paid Search' 
            WHEN medium ~ 'cpc|ppc' AND campaign ~ '^\[D\].+'
                THEN 'Paid Display'
            WHEN medium ~ 'cpc|ppc' AND campaign ~ '^\[V\].+'
                THEN 'Paid Video'
            WHEN (medium ~ 'social|linkedin') OR (medium IS NOT NULL AND source ~ 'linkedin|facebook|social media|twitter')
                THEN 'Paid Social'
            WHEN medium ~ 'cpc|ppc' AND campaign IS NOT NULL
                THEN 'Paid Other'
            WHEN medium ~ 'email|newsletter' OR source ~ 'email|newsletter'
                THEN 'Email'
            -- created a seperate channel grouping for AI sources to track our visibility therre
            WHEN campaign IS NULL AND medium IS NULL AND content IS NULL AND term IS NULL AND affiliate_code IS NULL
                AND (source ~ 'openai|chatgpt|gpt|gemini|bard|copilot|perplexity|neeva|writesonic|copy\.ai|astastic|outrider|nimble|bnngpt|edgeservices|claude|grok' OR referrer_domain ~'(^|[./])(openai|chatgpt|gpt|gemini|bard|copilot|perplexity|neeva|writesonic|copy\.ai|astastic|outrider|nimble|bnngpt|edgeservices|claude|grok)([./]|$)')
                THEN 'AI'
            WHEN campaign IS NULL AND source IS NULL AND medium IS NULL AND content IS NULL AND term IS NULL AND gclid IS NULL AND affiliate_code IS NULL 
                AND referrer_domain ~ '(^|[./])(google|bing|yandex|duckduckgo|search\.brave\.com|startpage\.com|yahoo|presearch.com|baidu.com)([./]|$)' 
                THEN 'Organic Search'
            WHEN campaign IS NULL AND source IS NULL AND medium IS NULL AND content IS NULL AND term IS NULL AND gclid IS NULL AND affiliate_code IS NULL
                AND referrer_domain ~ '(^|[./])(facebook|linkedin|twitter|t.co|instagram|telegram|skype|reddit)([./]|$)'
                THEN 'Organic Social'
            WHEN campaign IS NULL AND source IS NULL AND medium IS NULL AND content IS NULL AND term IS NULL AND gclid IS NULL AND affiliate_code IS NULL
                AND referrer_domain ~ '(^|[./])(youtube|tiktok)([./]|$)' 
                THEN 'Organic Video'
            WHEN affiliate_code IS NOT NULL
                THEN 'Affiliate'
            WHEN (campaign IS NULL AND source IS NULL AND medium IS NULL AND content IS NULL AND term IS NULL AND gclid IS NULL AND affiliate_code IS NULL)
            		-- filtered out false referrals, such as our own website, authentication and payment sites. Assigned such to Direct
                AND (referrer_domain IS NULL OR referrer_domain ~ '^(acs|acs2|3ds)([./-]|$)' OR referrer_domain ~ '(^|[./])(cherryservers.com|coingate|paypal|paysera|idenfy|3dsecure|cardinalcommerce|wibmo|proxify|shareasale-analytics|rsa3dsauth)([./]|$)')
                THEN 'Direct'
            WHEN campaign IS NULL AND source IS NULL AND medium IS NULL AND content IS NULL AND term IS NULL AND gclid IS NULL AND affiliate_code IS NULL AND referrer_domain IS NOT NULL
                THEN 'Referral'
            ELSE 'Not set'
        END AS default_channel_grouping,
        ROW_NUMBER() OVER(PARTITION BY anonymous_id ORDER BY session_start_time) AS session_by_anonymous
    FROM sessions s
),
model AS (
    SELECT
        anonymous_id,
        session_id,
        -- adjusted to prevent identical attribution_id in case of multiple tags in valued as NULL
        MD5(CONCAT_WS('|',
            COALESCE(campaign, 'NULL'),
            COALESCE(source, 'NULL'),
            COALESCE(medium, 'NULL'),
            COALESCE(content, 'NULL'),
            COALESCE(term, 'NULL'),
            COALESCE(referrer_domain, 'NULL'),
            COALESCE(affiliate_code, 'NULL'),
            COALESCE(default_channel_grouping, 'NULL')
        )) AS attribution_id,
        pageviews,
        session_start_time,
        session_end_time,
        default_channel_grouping,
        campaign,
        source,
        medium,
        content,
        term,
        referrer_domain,
        gclid,
        affiliate_code,
        first_page_visited,
        session_by_anonymous
    FROM tagged_sessions
)
SELECT *
FROM model