
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
    GROUP BY customerid);

SELECT COUNT(*) AS all
FROM fraud_tbl;