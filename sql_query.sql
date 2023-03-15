CREATE VIEW all_transactions AS 
  SELECT * FROM ( 
    SELECT id, client_id, value, date_time, 'in' AS transaction_type FROM transactions_in
    UNION ALL
    SELECT id, client_id, value, date_time, 'out' AS transaction_type FROM transactions_out) AS transactions;

CREATE VIEW fraudulent_transactions AS 
  SELECT * FROM (
  SELECT client_id, table_datatime_previous.id AS id_transaction, value, date_time, transaction_type
    FROM (
      SELECT id, client_id, value, date_time, transaction_type,
        LAG(date_time) OVER (
          PARTITION BY client_id
          ORDER BY date_time
        ) AS datatime_previous
      FROM all_transactions
    ) AS table_datatime_previous
    JOIN clients ON table_datatime_previous.client_id = clients.id
    WHERE datediff(SECOND, datatime_previous, date_time) < 120
    ) AS frauds

SELECT *
INTO frauds_transactions_out 
FROM fraudulent_transactions
WHERE transaction_type = 'out';

SELECT *
INTO frauds_transactions_in 
FROM fraudulent_transactions
WHERE transaction_type = 'in';

ALTER TABLE frauds_transactions_in
ADD CONSTRAINT fk_clients_id
FOREIGN KEY (client_id)
REFERENCES clients (id);

ALTER TABLE frauds_transactions_out
ADD CONSTRAINT fk_clients_id_out
FOREIGN KEY (client_id)
REFERENCES clients (id);

ALTER TABLE frauds_transactions_out
ADD CONSTRAINT fk_transactions_id_out
FOREIGN KEY (id_transaction)
REFERENCES transactions_out (id);

ALTER TABLE frauds_transactions_in
ADD CONSTRAINT fk_transactions_id_in
FOREIGN KEY (id_transaction)
REFERENCES transactions_in (id);

SELECT * FROM frauds_transactions_in ORDER BY client_id;
SELECT * FROM frauds_transactions_out ORDER BY client_id;

CREATE VIEW frauds_by_client AS
    SELECT c.name, c.id, COUNT(ft.id_transaction) as qty_of_transactions, SUM(value) as value_total_frauds
    FROM  fraudulent_transactions ft
    JOIN  clients c
    ON c.id = ft.client_id 
    GROUP BY ft.client_id, c.name, c.id;

SELECT * 
FROM frauds_by_client
ORDER BY qty_of_transactions DESC;

SELECT * 
FROM frauds_by_client 
ORDER BY value_total_frauds DESC;

CREATE VIEW frauds_by_state AS
    SELECT c.state, COUNT(ft.client_id) as qty_of_frauds
    FROM fraudulent_transactions ft 
    JOIN clients c 
    ON c.id = ft.client_id 
    GROUP BY c.state;

SELECT * 
FROM frauds_by_state
ORDER BY qty_of_frauds DESC;

CREATE VIEW sum_of_frauds AS
    SELECT transaction_type, SUM(value) as sum_value
    FROM  fraudulent_transactions
    GROUP BY transaction_type;

SELECT * 
FROM sum_of_frauds
ORDER BY sum_value DESC;

CREATE VIEW count_of_frauds AS
    SELECT transaction_type, COUNT(value) as qty_of_transactions
    FROM  fraudulent_transactions
    GROUP BY transaction_type;

SELECT * 
FROM count_of_frauds
ORDER BY qty_of_transactions DESC;

CREATE VIEW frauds_by_year AS
    SELECT YEAR(date_time) AS Year, COUNT(*) AS frauds_by_year
    FROM fraudulent_transactions
    GROUP BY YEAR(date_time);

SELECT * 
FROM frauds_by_year
ORDER BY Year DESC;

CREATE VIEW frauds_by_shift_and_year AS
    SELECT DATEPART(year, date_time) AS Year, 
        CASE 
            WHEN DATEPART(hour, date_time) BETWEEN 0 AND 5 THEN '0-6 hours'
            WHEN DATEPART(hour, date_time) BETWEEN 6 AND 11 THEN '6-12 hours'
            WHEN DATEPART(hour, date_time) BETWEEN 12 AND 17 THEN '12-18 hours'
            WHEN DATEPART(hour, date_time) BETWEEN 18 AND 23 THEN '18-24 hours'
        END AS transaction_time,
        COUNT(*) AS qty_of_transactions
    FROM fraudulent_transactions
    GROUP BY DATEPART(year, date_time), 
            CASE 
                WHEN DATEPART(hour, date_time) BETWEEN 0 AND 5 THEN '0-6 hours'
                WHEN DATEPART(hour, date_time) BETWEEN 6 AND 11 THEN '6-12 hours'
                WHEN DATEPART(hour, date_time) BETWEEN 12 AND 17 THEN '12-18 hours'
                WHEN DATEPART(hour, date_time) BETWEEN 18 AND 23 THEN '18-24 hours'
            END;

SELECT * 
FROM frauds_by_shift_and_year
ORDER BY Year;

CREATE VIEW frauds_by_month AS
    SELECT MONTH(date_time) AS month, COUNT(id_transaction) AS qty_of_frauds
    FROM fraudulent_transactions
    GROUP BY MONTH(date_time);

SELECT * 
FROM frauds_by_month
ORDER BY qty_of_frauds DESC;
