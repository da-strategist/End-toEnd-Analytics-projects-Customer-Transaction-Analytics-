---Here we shall carry out all data transaformation tasks which includes the following:
---looking out for duplicates
---cleaning individual columns
---performing joins where necessary
/*
 
 */
---Table 1 Foreign customer table
--- we shall begin by extracting the columns we need into a new table
DROP TABLE forcus_ext;

CREATE TABLE forcus_ext AS
SELECT customerid,
    cus_dob,
    cus_gender,
    cus_location,
    cus_balance,
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
        END,
        '[^A-Za-z\s]+',
        ' ',
        'g'
    ) AS city_cleaned,
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
        END,
        '[^A-Za-z\s]+',
        ' ',
        'g'
    ) 
    
    
--next we drop the old cus_location column

ALTER TABLE forcus_ext 
DROP COLUMN cus_location 
---next we move on to correct other anomalies found in the cus_city column e.g instances where 
---we have values such as kuala lumpur kualalumpur etc


UPDATE forcus_ext
SET cus_city = TRIM(
        CASE
            WHEN cus_city = 'SULTANATE OFOMAN' THEN 'SULTANATE OF OMAN'
            WHEN cus_city IN('KUALALUMPUR', 'KUALA LAMPUR') THEN 'KUALA LUMPUR'
            WHEN cus_city = 'DARES SALAAM' THEN 'DAR ES SALAAM'
            WHEN cus_city = 'DUBAI UAE' THEN 'DUBAI'
            WHEN cus_city IN('BIETIGHEIM', 'BIETIGHEIM BISSINGEN') THEN 'BIETIGHEIM-BISSINGEN'
            WHEN cus_city IN (
                'AL JUBAIL INSUDTRIAL',
                'JUBAILI IND CITY',
                'JUBAIL'
            ) THEN 'AL JUBAIL'
            WHEN cus_city = 'KUWAIT CITY' THEN 'KUWAIT'
            WHEN cus_city = 'KHOBAR' THEN 'AL KHOBAR'
            ELSE cus_city
        END
    ) --next we look out for duplicate records
    WITH dup_check_CTE AS(
        SELECT *,
            RANK() OVER (
                PARTITION BY customerid
                ORDER BY customerid DESC
            ) AS rank_
        FROM forcus_ext
    )
SELECT *
FROM dup_check_CTE
WHERE rank_ > 1


SELECT cus_city,
    COUNT(customerid) AS tot_count
FROM forcus_ext
GROUP BY cus_city
ORDER BY 2 DESC 


---next we take a look at our date column [cus_dob]



SELECT MIN(cus_dob) AS min_dob,
    MAX(cus_dob) AS max_dob
FROM forcus_ext;
---from the above query, it is clear our dataset is free of duplicate values
/*now we create a new column called cus_type and populate with the value 'foreign' 
 this helps us trace anomalies when we perform our merge operations
 
 */

ALTER TABLE forcus_ext
ADD COLUMN cus_cat VARCHAR(10);

UPDATE forcus_ext
SET cus_cat = 'FOREIGN' 

---now we move on to the customer table
----starting with the customer dob column
--first we  drop the previously created staging customer table
---DROP TABLE staging_cus_tbl

CREATE TABLE de_learner.local_cus_ext AS 
WITH dob_cte AS 
(
    SELECT customer_id,
            cus_gender,
            cus_location,
            cus_balance,
            cus_dob,
            EXTRACT(
                YEAR
                FROM cus_dob
            ) AS cus_yr
        FROM customers
    ),
    stag_tbl AS (
        SELECT customer_id,
            cus_gender,
            cus_location,
            cus_dob,
            cus_balance
        FROM dob_cte
        WHERE cus_yr > 1800
            AND cus_yr <= 2004
    )
    SELECT *
    FROM stag_tbl;


---customer location column
--here we add a new column city to hold values for all city names
ALTER TABLE local_cus_ext
ADD COLUMN city VARCHAR(20) 

--we also add a new column to hold customer type

ALTER TABLE local_cus_ext
ADD cus_cat VARCHAR(10);


UPDATE local_cus_ext
SET cus_cat = 'LOCAL'

alter table local_cus_ext
ADD age INT 


---here we update the cutomer city column

UPDATE local_cus_ext
SET city = CASE
        WHEN cus_location = 'BANGALORE NORTH' THEN 'BANGALORE'
        WHEN cus_location = 'GHINGHARTOLA TEH BAGESHWAR DIST' THEN 'BAGESHWAR'
        WHEN cus_location = 'MIRABHAYANDER THANE MIRA ROAD' THEN 'MIRA ROAD E'
        WHEN UPPER(cus_location) IN (
            'NEW DELHI',
            'MIRA ROAD E',
            'WEST MUMBAI',
            'RANGA REDDY',
            'GUNTUR DIST',
            'TIRUPATI (RURAL)',
            'KALYAN W',
            'THANE W',
            'GREATER NOIDA',
            'ARIYALUR DISTRICT',
            'BHILAI I',
            'AMBALA CANTT',
            'NAGAI DT',
            'REWARI (HARYANA)',
            'KANJURMARG EAST',
            'KARUR DT',
            'MEDAK DIST'
        ) THEN regexp_replace(cus_location, '[^a-zA-Z ]', '', 'g')
        WHEN array_length(string_to_array(cus_location, ' '), 1) = 1 THEN split_part(cus_location, ' ', 1)
        WHEN array_length(string_to_array(cus_location, ' '), 1) = 5 THEN split_part(cus_location, ' ', 5)
        WHEN array_length(string_to_array(cus_location, ' '), 1) = 4 THEN split_part(cus_location, ' ', 4)
        WHEN array_length(string_to_array(cus_location, ' '), 1) = 3 THEN split_part(cus_location, ' ', 3)
        WHEN array_length(string_to_array(cus_location, ' '), 1) = 2 THEN split_part(cus_location, ' ', 2)
    END
SELECT *
FROM local_cus_ext;

SELECT *
FROM forcus_ext 


---now we perform a union of both table to combione the datasets
DROP TABLE customer_tbl;

CREATE TABLE staging_customer_tbl AS
SELECT customer_id,
    cus_gender,
    city,
    cus_dob,
    cus_cat,
    cus_balance,
    age
FROM local_cus_ext

union

SELECT customerid,
    cus_gender,
    cus_city,
    cus_dob,
    cus_cat,
    cus_balance,
    age
FROM forcus_ext

SELECT *
FROM customer_tbl 

--now we move on to the transaction table
SELECT *
FROM forcus;


SELECT *
FROM transactions 

--we clean the time column

ALTER TABLE transactions
ADD COLUMN txntime TIME

UPDATE transactions 
----the function LPAD is used to add 00 to time values with less than 6 characters
SET txntime = to_timestamp(
        LPAD(transactiontime::text, 6, '0'),
        'HH24MISS'
    )

---now we create our new transactions table but running a union of both tables

CREATE TABLE staging_txn_tbl AS

SELECT transactionid,
    customerid,
    transactiondate,
    txntime,
    transactionamount
FROM transactions
UNION
SELECT transactionid,
    customerid,
    transactiondate,
    transactiontime,
    transactionamount
FROM forcus;


SELECT * FROM staging_customer_tbl;
SELECT * FROM staging_txn_tbl
