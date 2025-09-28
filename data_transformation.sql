
---Here we shall carry out all data transaformation tasks which includes the following:

---looking out for duplicates
---cleaning individual columns
---performing joins where necessary

/*

*/

---Table 1 Foreign customer table

--- we shall begin by extracting the columns we need into a new table

CREATE TABLE forcus_ext 
AS
SELECT customerid,
        cus_dob,
        cus_gender,
        cus_location,
        age        
FROM forcus;


--we write our syntax to clean the customer location column

SELECT cus_location,
        regexp_replace(
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
        END, '[^A-Za-z\s]+', ' ', 'g') AS city_cleaned, 
        LENGTH(cus_location) AS str_count
FROM forcus_ext

---now we add a new city column and drop the old cus_location column

ALTER TABLE forcus_ext
ADD COLUMN cus_city VARCHAR(20)


---we populate the newly created column with values

UPDATE forcus_ext
SET cus_city = regexp_replace(
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
        END, '[^A-Za-z\s]+', ' ', 'g')

--next we drop the old cus_location column


SELECT * FROM forcus_ext


----cleaning customer dob column


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
        ORDER BY 3; 
    


WITH loc_ext_CTE AS (
    SELECT cus_location,
    regexp_replace(
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
        END, '[^A-Za-z\s]+', ' ', 'g') AS city_cleaned, 
        LENGTH(cus_location) AS str_count
    FROM forcus ---ORDER BY 4 DESC
)
SELECT DISTINCT cus_location,
    city_cleaned, str_count
FROM loc_ext_CTE
ORDER BY 3 DESC


 SELECT * FROM forcus



