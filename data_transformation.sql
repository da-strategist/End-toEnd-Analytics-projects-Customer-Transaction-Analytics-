
---we kick off our data transaformation with the customer table 

/*

*/

---cleaning customer dob column

/*
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
*/

SELECT * 
FROM staging_cus_tbl

