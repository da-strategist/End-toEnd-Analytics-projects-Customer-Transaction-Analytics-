
---we kick off our data transaformation with the foreign customer table 

/*

*/


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

SELECT DISTINCT customerid
                ,COUNT(transactionid) AS tottxn, 
                MAX(transactiondate) AS lasttxndate
                ,MIN(transactiondate) AS earliesttxndate
    FROM de_learner.fraud_tbl
    GROUP BY customerid
    HAVING COUNT(transactionid) > 1