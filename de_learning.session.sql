

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

--here we add a new column city to hold values for all city names
--ALTER TABLE staging_cus_tbl
--ADD COLUMN city VARCHAR(20)




--UPDATE staging_cus_tbl
--SET city = 


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
