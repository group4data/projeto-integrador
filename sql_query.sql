CREATE VIEW all_transactions AS 
  SELECT * FROM ( 
    SELECT id, cliente_id, valor, data_hora, 'in' AS tipo_transacao FROM transactions_in
    UNION ALL
    SELECT id, cliente_id, valor, data_hora, 'out' AS tipo_transacao FROM transactions_out) AS transactions;


CREATE VIEW transactions_date_previous AS
  SELECT * FROM (
  SELECT id, cliente_id, valor, data_hora, tipo_transacao, 
  LAG(data_hora) OVER (
      PARTITION BY cliente_id
      ORDER BY data_hora
    ) AS datatime_previous
  FROM all_transactions) AS date_previous;


SELECT cliente_id, transactions_date_previous.id AS transacao_id, valor, data_hora, tipo_transacao
INTO fraudulent_transactions
FROM transactions_date_previous
JOIN clientes ON cliente_id = clientes.id
WHERE datediff(SECOND, datatime_previous, data_hora) < 120
ORDER BY cliente_id, data_hora;


SELECT DISTINCT(cliente_id), clientes.nome, clientes.estado FROM fraudulent_transactions JOIN clientes ON cliente_id = clientes.id; 


SELECT * FROM clientes;
SELECT * FROM dbo.transactions_in;
SELECT * FROM dbo.transactions_out;
SELECT * FROM all_transactions;
SELECT * FROM fraudulent_transactions;

ALTER TABLE fraudulent_transactions
ADD CONSTRAINT fk_clientes_cliente_id
FOREIGN KEY (cliente_id)
REFERENCES clientes (id);