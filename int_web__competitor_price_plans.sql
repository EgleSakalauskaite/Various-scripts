WITH
countries AS (
    SELECT * FROM {{ ref('DimCountries') }}
),
all_plans AS (
    SELECT * FROM {{ ref('stg_web__cherry') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_web__latitude_baremetal') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_web__latitude_vm') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_web__ovh_dedicated') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_web__ovh_virtual') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_web__hetzner') }}
),
location AS (
    SELECT
        ap.extracted_date,
        ap.company,
        ap.product_type,
        ap.plan_name,
        ap.plan_id,
        MD5(CONCAT_WS('|', ap.plan_id, c.country)) AS plan_by_loc_id,
        ap.cpu_model,
        coalesce(ap.cpu_count, 1) AS cpu_count,
        ap.cores_count,
        ap.frequency,
        ap.ram,
        ap.storage,
        ap.gpu,
        replace(ap.bandwidth, ' ', '') AS bandwidth,
        ap.traffic,
        c.country AS location,
        c.region AS region,
        ap.availability,
        ap.setup_fee,
        ap.price_monthly,
        ap.price_hourly,
        ap.currency
    FROM all_plans ap
    LEFT JOIN countries c
        ON ap.location = c.iso_2_code OR ap.location = c.country
),
vendor AS (
    SELECT *,
        CASE
            WHEN cpu_model ~* '\m(AMD|EPYC|Ryzen|Threadripper)\M'
                THEN 'AMD'
            WHEN cpu_model ~* '\m(Intel|Xeon|Core|Pentium|Celeron)\M'
                THEN 'Intel'
            WHEN cpu_model ~* '\mE[3457]?-[0-9]{3,4}' OR cpu_model ~* '\m[Ii][3579]-[0-9]{4,5}'
                THEN 'Intel'
            ELSE NULL
        END AS cpu_vendor
    FROM location
),
model AS (
    SELECT *,
        CASE
            WHEN region = 'Europe' OR location = 'United States'
                THEN 'EU/US'
            WHEN region = 'Americas'
                THEN 'Americas (non US)'
            ELSE region
        END AS custom_region,
        CASE
            WHEN cpu_vendor NOT IN ('Intel', 'AMD')
                THEN NULL
            WHEN cpu_model ~* '\mE[57]-[0-9]'
                THEN 'Xeon ' || upper((regexp_match(cpu_model, '\m(E[57])-', 'i'))[1])
            WHEN cpu_model ~* '\mE3[\s-]?[0-9]|Xeon-?E3\M'
                THEN 'Xeon E3'
            WHEN cpu_model ~* 'Xeon[\s-]E\M[\s-]+[0-9]{4}|\mE-[0-9]{4}'
                THEN 'Xeon E'
            WHEN cpu_model ~* 'Xeon[\s-]D\M'
                THEN 'Xeon D'
            WHEN cpu_model ~* 'platinum'
                THEN 'Xeon Platinum'
            WHEN cpu_model ~* 'gold'
                THEN 'Xeon Gold'
            WHEN cpu_model ~* 'silver'
                THEN 'Xeon Silver'
            WHEN cpu_model ~* 'bronze'
                THEN 'Xeon Bronze'
            -- Core Ultra MUST come before the Core i[3579] branch
            WHEN cpu_model ~* 'core[\s-]+ultra[\s-]+[3579]'
                THEN 'Core Ultra ' || (regexp_match(cpu_model, 'core[\s-]+ultra[\s-]+([3579])', 'i'))[1]
            WHEN cpu_vendor = 'Intel' AND (cpu_model ~* '\mcore\M.*\mi[3579]\M' OR cpu_model ~* '\mi[3579]-[0-9]{4,5}')
                THEN 'Core ' || lower((regexp_match(cpu_model, '\m([Ii][3579])', 'i'))[1])
            WHEN cpu_vendor = 'Intel' AND cpu_model ~* '\m6[0-9]{3}[A-Z]?\M'
                THEN 'Xeon 6'
            WHEN cpu_vendor = 'Intel' AND cpu_model ~* '\m5[0-9]{3}[A-Z]?\M'
                THEN 'Xeon 5'
            WHEN cpu_model ~* 'threadripper'
                THEN 'Threadripper PRO'
            WHEN cpu_model ~* '\mryzen\M'
                THEN 'Ryzen'
            WHEN cpu_model ~* '\mepyc\M' OR (cpu_vendor = 'AMD' AND cpu_model ~ '\m[479][0-9]{3}[A-Z]*\M')
                THEN 'EPYC'
            ELSE NULL
        END AS cpu_family,
        CASE
            WHEN cpu_vendor NOT IN ('Intel', 'AMD')
                THEN NULL
            WHEN cpu_model ~* '\mE[57]-[0-9]'
                THEN (regexp_match(cpu_model, '\mE[57]-([0-9]+[A-UW-Z]*)', 'i'))[1]
            WHEN cpu_model ~* '\mE3[\s-]?[0-9]|Xeon-?E3\M'
                THEN (regexp_match(cpu_model, '\mE3[\s-]?([0-9]+[A-UW-Z]*)', 'i'))[1]
            WHEN cpu_model ~* 'Xeon[\s-]E\M[\s-]+[0-9]{4}|\mE-[0-9]{4}'
                THEN (regexp_match(cpu_model, '(?:Xeon[\s-]E\M[\s-]+|\mE-)([0-9]+[A-Z]*)', 'i'))[1]
            WHEN cpu_model ~* 'Xeon[\s-]D\M'
                THEN (regexp_match(cpu_model, 'Xeon[\s-]D[\s-]+([0-9]+[A-Z]*)', 'i'))[1]
            WHEN cpu_model ~* 'platinum|gold|silver|bronze'
                THEN (regexp_match(cpu_model, '(?:platinum|gold|silver|bronze)\s+([0-9]+[A-Z+]*)', 'i'))[1]
            -- Core Ultra MUST come before the Core i[3579] branch
            WHEN cpu_model ~* 'core[\s-]+ultra[\s-]+[3579]'
                THEN (regexp_match(cpu_model, 'core[\s-]+ultra[\s-]+[3579][\s-]+([0-9]{3}[A-Z]*)', 'i'))[1]
            WHEN cpu_vendor = 'Intel' AND (cpu_model ~* '\mcore\M.*\mi[3579]\M' OR cpu_model ~* '\mi[3579]-[0-9]{4,5}')
                THEN (regexp_match(cpu_model, '\mi[3579]-([0-9]+[A-Z]*)', 'i'))[1]
            WHEN cpu_vendor = 'Intel' AND cpu_model ~* '\m[56][0-9]{3}[A-Z]?\M'
                THEN (regexp_match(cpu_model, '\m([56][0-9]{3}[A-Z]?)\M'))[1]
            WHEN cpu_vendor = 'AMD'
                THEN (regexp_match(cpu_model, '\m([0-9]{4,5}[A-Z0-9]*)\M', 'i'))[1]
            ELSE NULL
        END AS cpu_model_number
    FROM vendor
)
SELECT *,
    COALESCE(
        -- Intel: explicit "v2" / "(Gen 11)" markers
        (REGEXP_MATCH(cpu_model, 'v([0-9]+)\s*$'))[1]::INT,
        (REGEXP_MATCH(cpu_model, '\(Gen\s+([0-9]+)\)'))[1]::INT,

        -- Intel Xeon Scalable + Xeon 6:
        -- Xeon 6 = 4-digit SKU starting with 6 and ending in P or E (Granite Rapids / Sierra Forest)
        -- Otherwise, second digit of the SKU = generation (Skylake=1, Cascade=2, Ice=3, Sapphire=4, Emerald=5)
        CASE
            WHEN cpu_vendor = 'Intel' AND cpu_family IN ('Xeon Bronze','Xeon Silver','Xeon Gold','Xeon Platinum','Xeon 5','Xeon 6')
                THEN CASE
                    WHEN cpu_model_number ~* '^6[0-9]{3}[PE]$'              THEN 6  -- Xeon 6 (Granite Rapids / Sierra Forest), wins even if labeled "Gold"
                    WHEN cpu_family = 'Xeon 6'                              THEN 6
                    WHEN cpu_model_number ~ '^[3-9]1[0-9]{2}'               THEN 1  -- Skylake-SP
                    WHEN cpu_model_number ~ '^[3-9]2[0-9]{2}'               THEN 2  -- Cascade Lake
                    WHEN cpu_model_number ~ '^[3-9]3[0-9]{2}'               THEN 3  -- Ice Lake / Cooper Lake
                    WHEN cpu_model_number ~ '^[3-9]4[0-9]{2}'               THEN 4  -- Sapphire Rapids
                    WHEN cpu_model_number ~ '^[3-9]5[0-9]{2}'               THEN 5  -- Emerald Rapids (incl. 5515+, 6542Y, 6554S)
                END
        END,

        -- Intel Xeon-D: by model-number range
        CASE
            WHEN cpu_vendor = 'Intel' AND cpu_family = 'Xeon D'
                THEN CASE
                    WHEN cpu_model_number ~ '^15'                           THEN 1  -- Broadwell-DE  (1520, 1521, 1540)
                    WHEN cpu_model_number ~ '^2[12]'                        THEN 2  -- Skylake-D     (2123IT, 2141I)
                    WHEN cpu_model_number ~ '^(17|27)'                      THEN 3  -- Ice Lake-D
                    WHEN cpu_model_number ~ '^(18|28)'                      THEN 4  -- Granite Rapids-D
                END
        END,

        -- Intel Xeon-E (entry server): by model-number range
        CASE
            WHEN cpu_vendor = 'Intel' AND cpu_family = 'Xeon E'
                THEN CASE
                    WHEN cpu_model_number ~ '^21'                           THEN 1  -- E-21xx Coffee Lake          (2136)
                    WHEN cpu_model_number ~ '^22'                           THEN 2  -- E-22xx Coffee Lake refresh  (2274G, 2288G)
                    WHEN cpu_model_number ~ '^23'                           THEN 3  -- E-23xx Rocket Lake          (2386G, 2388G)
                    WHEN cpu_model_number ~ '^24'                           THEN 4  -- E-24xx (future)
                END
        END,

        -- Intel Core i3/i5/i7/i9: leading 1-2 digits of model number = generation
        CASE
            WHEN cpu_vendor = 'Intel' AND cpu_family ~ '^Core i[3579]$'
                THEN CASE
                    WHEN cpu_model_number ~ '^1[0-9]{4}'                    THEN substring(cpu_model_number from '^(1[0-9])')::INT  -- 10000+ → 10-19
                    WHEN cpu_model_number ~ '^[2-9][0-9]{3}'                THEN substring(cpu_model_number from '^([2-9])')::INT  -- 2000-9999 → 2-9
                END
        END,

        -- Intel Core Ultra: Series 1 = Meteor Lake (1xx), Series 2 = Arrow/Lunar Lake (2xx)
        CASE
            WHEN cpu_vendor = 'Intel' AND cpu_family ~ '^Core Ultra'
                THEN substring(cpu_model_number from '^([0-9])')::INT
        END,

        -- AMD EPYC: Zen architecture by model-number family
        CASE
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^7[0-9]{2}1[a-z]*$' THEN 1  -- Naples
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^7[0-9]{2}2[a-z]*$' THEN 2  -- Rome
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^7[0-9]{2}3[a-z]*$' THEN 3  -- Milan
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^8[0-9]{3}[a-z]*$'  THEN 4  -- Siena
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^9[0-9]{2}4[a-z]*$' THEN 4  -- Genoa/Bergamo
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^9[0-9]{2}5[a-z]*$' THEN 5  -- Turin
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^4[0-9]{2}4[a-z]*$' THEN 4  -- EPYC 4004
            WHEN cpu_vendor = 'AMD' AND cpu_family = 'EPYC' AND cpu_model_number ~* '^4[0-9]{2}5[a-z]*$' THEN 5  -- EPYC 4005

            -- AMD Ryzen / Threadripper: thousands digit → Zen architecture
            WHEN cpu_vendor = 'AMD' AND cpu_family IN ('Ryzen', 'Threadripper PRO')
                THEN CASE substring(cpu_model_number from '^([0-9])')
                    WHEN '1' THEN 1
                    WHEN '2' THEN 1
                    WHEN '3' THEN 2
                    WHEN '4' THEN 2
                    WHEN '5' THEN 3
                    WHEN '6' THEN 3
                    WHEN '7' THEN 4
                    WHEN '8' THEN 4
                    WHEN '9' THEN 5
                END
        END
    ) AS cpu_generation
FROM model