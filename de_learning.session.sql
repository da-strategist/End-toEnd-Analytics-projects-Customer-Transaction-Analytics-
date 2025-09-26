

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

SELECT *
FROM customers;



SELECT COUNT(*)
FROM customers;

SELECT MIN(cus_dob),
        MAX(cus_dob)
FROM customers
