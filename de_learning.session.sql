

/*
SELECT COUNT(*) AS grouped
FROM(
    SELECT DISTINCT customerid
                ,COUNT(transactionid) AS tottxn, 
                MAX(transactiondate) AS lasttxndate
                ,MIN(transactiondate) AS earliesttxndate
    FROM de_learner.fraud_tbl
    GROUP BY customerid
    HAVING COUNT(transactionid) > 1);


SELECT COUNT(*) AS all
FROM fraud_tbl;

SELECT *
FROM fraud_tbl
WHERE customerid = 'C6131774';

*/

/*
SELECT DISTINCT customerid
                ,COUNT(transactionid) AS tottxn, 
                MAX(transactiondate) AS lasttxndate
                ,MIN(transactiondate) AS earliesttxndate
    FROM de_learner.fraud_tbl
    GROUP BY customerid
    HAVING COUNT(transactionid) > 1;

SELECT *
FROM fraud_tbl;

SELECT DISTINCT isanomaly, COUNT(isanomaly) AS count_
FROM fraud_tbl
GROUP BY isanomaly

*/
--SELECT * 
--FROM forcus


--SELECT COUNT(*)
--FROM forcus;



--next we work on the customer location column by extraxcting the name of cities
/*
SELECT 
    cus_location,
    CASE 
        --WHEN cus_location = 'Bangalore North' THEN 'Bangalore'
        WHEN UPPER(cus_location) IN ('New delhi', 'Mira Road E',
                            'West Mumbai', 'Ranga Reddy',
                            'Guntur Dist', 'Tirupati (Rural)',
                            'Kalyan W', 'Thane W', 'Greater Noida'
                            ,'Ariyalur District', 'Bhilai I'
                            ,'Ambala Cantt', 'Nagai DT',
                            'Rewari (Haryana)') 
                            THEN cus_location
        WHEN position(' ' IN cus_location) > 0
        THEN split_part(
            cus_location, 
            ' ', 
            array_length(string_to_array(cus_location, ' '), 1)
        )
        
        ELSE cus_location
    END AS city
FROM staging_cus_tbl
ORDER BY 2 DESC;

*/







SELECT 
    cus_location,
    CASE 
       -- WHEN cus_location = 'NEW'
        WHEN position(' ' IN cus_location) > 0
        THEN split_part(
            cus_location, 
            ' ', 
            array_length(string_to_array(cus_location, ' '), 1)
        )
        
        ELSE cus_location
    END AS city,
    CASE    WHEN cus_location = 'BANGALORE NORTH' THEN 'BANGALORE'
            WHEN UPPER(cus_location) IN ('NEW DELHI', 'MIRA ROAD E',
                            'WEST MUMBAI', 'RANGA REDDY',
                            'GUNTUR DIST', 'TIRUPATI (RURAL)',
                            'KALYAN W', 'THANE W', 'GREATER NOIDA',
                            'ARIYALUR DISTRICT', 'BHILAI I',
                            'AMBALA CANTT', 'NAGAI DT',
                            'REWARI (HARYANA)') 
                            THEN cus_location
        WHEN position(' ' IN cus_location) > 0
        THEN split_part(
            cus_location, 
            ' ', 
            array_length(string_to_array(cus_location, ' '), 1)
        )
        ELSE cus_location
        END AS city_02
FROM staging_cus_tbl
ORDER BY 1 ASC


/*
WITH normalized AS (
    SELECT
        cus_location,
        trim(regexp_replace(cus_location, '\s+', ' ', 'g')) AS loc_norm,
        upper(trim(regexp_replace(cus_location, '\s+', ' ', 'g'))) AS loc_u
    FROM staging_cus_tbl
)
SELECT
    cus_location,
    CASE
        WHEN loc_u = 'BANGALORE NORTH' THEN 'Bangalore'
        WHEN loc_u IN (
            'NEW DELHI','MIRA ROAD E','WEST MUMBAI','RANGA REDDY','GUNTUR DIST',
            'TIRUPATI (RURAL)','KALYAN W','THANE W','GREATER NOIDA',
            'ARIYALUR DISTRICT','BHILAI I','AMBALA CANTT','NAGAI DT','REWARI (HARYANA)'
        ) THEN loc_norm
        WHEN position(' ' IN loc_norm) > 0 THEN loc_norm
        ELSE loc_norm
    END AS city
FROM normalized
ORDER BY city DESC;
*/


/*

WITH loc_ext_CTE AS (
    SELECT DISTINCT cus_location,
        CASE
            WHEN cus_location ~ '^\d+'
            OR cus_location ILIKE '%PO BOX%' THEN regexp_replace(
                cus_location,
                '.*\s([A-Za-z]+(?:\s[A-Za-z]+)*)$',
                '\1'
            )
            ELSE cus_location
        END AS city_ext,
        CASE
            WHEN cus_location LIKE '%PO BOX%'
            OR LENGTH(cus_location) > 20 THEN split_part(
                cus_location,
                ' ',
                array_length(
                    string_to_array(cus_location, ' '),
                    1
                )
            )
            ELSE cus_location
        END AS city_01,
        length(cus_location) AS str_count
    FROM forcus ---ORDER BY 4 DESC
)
SELECT cus_location,
    city_01,
    str_count,
    --CASE
       -- WHEN city_01 ~ '[^A-Za-z\s]' AND position(' ' IN city_01) >0
        --THEN TRIM(LEFT(city_01, position('[0-9]' IN city_01) - 1))
        --ELSE city_01
    --END AS city_02
    regexp_replace(city_01, '[^A-Za-z\s]+', ' ', 'g') AS city_03
FROM loc_ext_CTE
ORDER BY 1 ASC


*/


SELECT cus_location,
        CASE WHEN position(' ' IN cus_location) > 0
        THEN split_part(
            cus_location, 
            ' ',
            array_length(string_to_array(cus_location, ' '), 1)
        )
        ELSE cus_location
        END AS forcus_city01,
        CASE WHEN position(' ' IN cus_location) <= 1
        THEN split_part(cus_location, ' ', array_length(string_to_array(cus_location, ' '),1
        )
        ) END AS forcus_city02, 
        CASE WHEN position(' ' IN cus_location) <=2
        THEN split_part(cus_location, ' ', array_length(string_to_array(cus_location, ' '),1
        )
        ) END AS forcus_city03
FROM forcus;





WITH dummy_loc AS(
    SELECT DISTINCT cus_location,
        LENGTH(cus_location) AS str_len,
        --split_part(cus_location, ' ', 
        -- array_length(string_to_array(cus_location, ' '),1)) AS cityy
        ---string_to_array(cus_location, ' ') AS splitt,
        split_part(cus_location, ' ', 1) AS loc_1,
        split_part(cus_location, ' ', 2) AS loc_2,
        --   split_part(cus_location, ' ', 3) AS loc_3,
        -- split_part(cus_location, ' ', 4) AS loc_4,
        -- split_part(cus_location, ' ', 5) AS loc_5,
        array_length(string_to_array(cus_location, ' '), 1) AS parts_count,
        CASE
    -- If last word is a directional suffix, drop it
        WHEN lower(split_part(
             regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g'),
             ' ',
             array_length(string_to_array(
                 regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g'),
                 ' '), 1)
         )) IN ('w','e','n','s','west','east','north','south')
        THEN (SELECT string_agg(word, ' ')
              FROM (
                  SELECT unnest(string_to_array(
                      regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g'),
                      ' '
                  )) AS word
                  LIMIT (array_length(string_to_array(
                      regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g'),
                      ' '),1)-1)
              ) AS sub)
    -- Else apply the old 2-word + suffix rule
    WHEN array_length(string_to_array(
             regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g'), ' '), 1) = 2
         AND lower(split_part(
             regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g'), ' ', 2)
         ) IN ('dist', 'w', 'e', 'n', 's', 'north', 'south', 'east', 'west')
    THEN split_part(
             regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g'), ' ', 1)
    WHEN array_length(string_to_array(cus_location, ' '),1) = 1 THEN split_part(cus_location, ' ', 1)
    WHEN array_length(string_to_array(cus_location, ' '),1) = 5 THEN split_part(cus_location, ' ', 5) 
    WHEN array_length(string_to_array(cus_location, ' '),1) = 4 THEN split_part(cus_location, ' ', 4) 
    WHEN array_length(string_to_array(cus_location, ' '),1) = 3 THEN split_part(cus_location, ' ', 3) 
    -- Default: keep cleaned name
    ELSE regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g')
    END AS city_name
FROM local_cus_ext --ORDER BY 8 DESC
)
SELECT cus_location, city_name
FROM dummy_loc
ORDER BY 1 ASC
--WHERE parts_count = 2


        /*  CASE    WHEN cus_location = 'BANGALORE NORTH' THEN 'BANGALORE'
         WHEN UPPER(cus_location) IN ('NEW DELHI', 'MIRA ROAD E',
         'WEST MUMBAI', 'RANGA REDDY',
         'GUNTUR DIST', 'TIRUPATI (RURAL)',
         'KALYAN W', 'THANE W', 'GREATER NOIDA',
         'ARIYALUR DISTRICT', 'BHILAI I',
         'AMBALA CANTT', 'NAGAI DT',
         'REWARI (HARYANA)') 
         THEN cus_location
         WHEN position(' ' IN cus_location) > 0
         THEN split_part(
         cus_location, 
         ' ', 
         array_length(string_to_array(cus_location, ' '), 1)
         )
         ELSE cus_location
         END AS new_loc*/

SELECT DISTINCT cus_location, 
        CASE    WHEN cus_location = 'BANGALORE NORTH' THEN 'BANGALORE'
        WHEN cus_location = 'GHINGHARTOLA TEH BAGESHWAR DIST' THEN 'BAGESHWAR'
        WHEN cus_location = 'MIRABHAYANDER THANE MIRA ROAD' THEN 'MIRA ROAD E'
                WHEN UPPER(cus_location) IN ('NEW DELHI', 'MIRA ROAD E',
                            'WEST MUMBAI', 'RANGA REDDY',
                            'GUNTUR DIST', 'TIRUPATI (RURAL)',
                            'KALYAN W', 'THANE W', 'GREATER NOIDA',
                            'ARIYALUR DISTRICT', 'BHILAI I',
                            'AMBALA CANTT', 'NAGAI DT',
                            'REWARI (HARYANA)', 'KANJURMARG EAST', 'KARUR DT'
                            ,'MEDAK DIST'
                            ) 
                            THEN regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g')
                WHEN array_length(string_to_array(cus_location, ' '),1) = 1 THEN split_part(cus_location, ' ', 1)
                WHEN array_length(string_to_array(cus_location, ' '),1) = 5 THEN split_part(cus_location, ' ', 5) 
                WHEN array_length(string_to_array(cus_location, ' '),1) = 4 THEN split_part(cus_location, ' ', 4) 
                WHEN array_length(string_to_array(cus_location, ' '),1) = 3 THEN split_part(cus_location, ' ', 3) 
                WHEN array_length(string_to_array(cus_location, ' '),1) = 2 THEN split_part(cus_location, ' ', 2) 
                
         END    
FROM local_cus_ext
ORDER BY 1 ASC

 