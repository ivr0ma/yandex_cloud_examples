SELECT 
    currency,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM 
    transactions
WHERE 
    currency IN ('USD', 'EUR', 'RUB')
GROUP BY 
    currency
ORDER BY 
    total_amount DESC;

SELECT 
    DATE(transaction_date) AS day,
    COUNT(*) AS transaction_count,
    SUM(amount) AS daily_volume,
    AVG(amount) AS avg_transaction,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count
FROM 
    transactions
GROUP BY 
    DATE(transaction_date)
ORDER BY 
    day;