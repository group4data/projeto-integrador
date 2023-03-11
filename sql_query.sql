CREATE VIEW all_transactions AS 
  SELECT * FROM ( 
    SELECT id, cliente_id, valor, data_hora, 'in' AS tipo_transacao FROM transactions_in
    UNION ALL
    SELECT id, cliente_id, valor, data_hora, 'out' AS tipo_transacao FROM transactions_out) AS transactions;

CREATE VIEW fraudulent_transactions AS 
  SELECT * FROM (
  SELECT cliente_id, table_datatime_previous.id AS id_transaction, valor, data_hora, tipo_transacao
    FROM (
      SELECT id, cliente_id, valor, data_hora, tipo_transacao,
        LAG(data_hora) OVER (
          PARTITION BY cliente_id
          ORDER BY data_hora
        ) AS datatime_previous
      FROM all_transactions
    ) AS table_datatime_previous
    JOIN clientes ON table_datatime_previous.cliente_id = clientes.id
    WHERE datediff(SECOND, datatime_previous, data_hora) < 120
    ) AS frauds

SELECT DISTINCT(cliente_id), count(id_transaction) AS times_defrauded_client, sum(valor) AS valor_total_fraudes
INTO defrauded_clients
FROM fraudulent_transactions JOIN clientes ON cliente_id = clientes.id
GROUP BY cliente_id;

SELECT * FROM defrauded_clients ORDER BY cliente_id;

ALTER TABLE defrauded_clients
ADD CONSTRAINT fk_clientes_id
FOREIGN KEY (cliente_id)
REFERENCES clientes (id);