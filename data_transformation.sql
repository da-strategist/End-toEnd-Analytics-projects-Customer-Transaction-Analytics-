
---Here we shall carry out all data transaformation tasks which includes the following:

---looking out for duplicates
---cleaning individual columns
---performing joins where necessary

/*

*/

---cleaning customer dob column


CREATE TABLE de_learner.staging_cus_tbl AS

WITH dob_cte AS

(

SELECT customer_id
        ,cus_gender
        ,cus_location
        ,cus_balance
        ,cus_dob
        ,EXTRACT(YEAR FROM cus_dob) AS cus_yr
FROM customers

),

stag_tbl

AS

(
SELECT customer_id
        ,cus_gender
        ,cus_location
        ,cus_dob
        ,cus_balance
FROM dob_cte
WHERE cus_yr > 1800 AND cus_yr <= 2004

)

SELECT *
FROM stag_tbl;


---customer location column

--here we add a new column city to hold values for all city names
--ALTER TABLE staging_cus_tbl
--ADD COLUMN city VARCHAR(20)


UPDATE staging_cus_tbl
SET city = CASE    WHEN cus_location = 'BANGALORE NORTH' THEN 'BANGALORE'
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
        END



--now we move on to the foreign customer table


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



SELECT customerid,
        cus_dob,
        cus_gender,
        cus_location,
        cus_balance,
        age        
FROM forcus;

SELECT customer_id,
        cus_dob,
        cus_gender,
        city,
        cus_balance      
FROM staging_cus_tbl
/*
---cleaning foreign customer table

we start with the customer location column, the goal is to effectively extract 
city names. We shall achieve that with a CTE

*/

WITH forcus_CTE
AS 
    (
        SELECT *, length(cus_location) AS str_count,
                        CASE WHEN cus_location LIKE '%PO BOX%' OR LENGTH(cus_location) > 20 
                        THEN split_part(cus_location, ' ', array_length(string_to_array(cus_location, ' '),1
                        )
                        )
                        ELSE cus_location
                        END AS city_01
        FROM forcus
       --- ORDER BY 2 DESC
    )
        SELECT customerid, cus_location,
                str_count,
                city_01
        FROM forcus_CTE




