
---we kick off our data transaformation with the customer table 

/*

*/

---cleaning customer dob column
WITH dob_cte AS

(

SELECT customer_id,
        cus_dob,
        EXTRACT(YEAR FROM cus_dob) AS cus_yr
FROM customers

)

SELECT cus_yr,
        COUNT(cus_yr)
FROM dob_cte
GROUP BY cus_yr
ORDER BY 1 DESC