CREATE TABLE logs (
    log_id BIGINT,
    transaction_id BIGINT,
    category STRING,
    comment STRING,
    log_timestamp TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO logs VALUES
(1, 10001, 'Electronics', 'User bought a phone', '2023-03-10 14:26:00'),
(2, 10002, 'Travel', 'Flight ticket to Europe', '2023-03-10 14:31:00'),
(3, 99999, 'System', 'Unrelated transaction', '2023-03-10 14:40:00'),
(4, 10004, 'System', 'Weird currency ???', '2023-03-10 15:15:00'),
(5, 10006, 'Electronics', 'Laptop purchase', '2023-03-11 09:10:00'),
(6, 10007, 'Misc', 'Zero amount transaction?', '2023-03-11 10:11:00'),
(7, 10010, 'Electronics', 'Gaming console', '2023-03-12 11:06:00'),
(8, 10012, 'System', 'Negative sum, check fraud', '2023-03-12 11:16:00'),
(9, 10013, 'Other', 'Big sum transaction', '2023-03-13 09:10:00'),
(10, 10015, 'Travel', 'Train ticket in RUB', '2023-03-14 08:50:00'),
(11, 10001, 'Electronics', 'Repeat mention phone?', '2023-03-10 14:27:00'),
(12, 11111, 'System', 'No valid transaction link', '2023-03-14 08:55:00');

SELECT * FROM logs;