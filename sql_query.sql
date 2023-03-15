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

SELECT *
INTO frauds_transactions_out 
FROM fraudulent_transactions
WHERE tipo_transacao = 'out';

SELECT *
INTO frauds_transactions_in 
FROM fraudulent_transactions
WHERE tipo_transacao = 'in';

ALTER TABLE frauds_transactions_in
ADD CONSTRAINT fk_clientes_id
FOREIGN KEY (cliente_id)
REFERENCES clientes (id);

ALTER TABLE frauds_transactions_out
ADD CONSTRAINT fk_clientes_id_out
FOREIGN KEY (cliente_id)
REFERENCES clientes (id);

ALTER TABLE frauds_transactions_out
ADD CONSTRAINT fk_transactions_id_out
FOREIGN KEY (id_transaction)
REFERENCES transactions_out (id);

ALTER TABLE frauds_transactions_in
ADD CONSTRAINT fk_transactions_id_in
FOREIGN KEY (id_transaction)
REFERENCES transactions_in (id);

SELECT * FROM frauds_transactions_in ORDER BY cliente_id;
SELECT * FROM frauds_transactions_out ORDER BY cliente_id;

CREATE VIEW frauds_by_client AS
    SELECT c.nome, COUNT(ft.id_transaction) as quantidade_transacoes
    FROM  fraudulent_transactions ft
    JOIN  clientes c
    ON c.id = ft.cliente_id 
    GROUP BY ft.cliente_id, c.nome;

SELECT * 
FROM frauds_by_client
ORDER BY quantidade_transacoes DESC;

CREATE VIEW sum_frauds_by_client AS
    SELECT c.nome, SUM(ft.valor) as valor_total_fraudes
    FROM  fraudulent_transactions ft
    JOIN  clientes c
    ON c.id = ft.cliente_id 
    GROUP BY ft.cliente_id, c.nome;

SELECT * 
FROM sum_frauds_by_client
ORDER BY valor_total_fraudes DESC;

CREATE VIEW frauds_by_state AS
    SELECT c.estado, COUNT(ft.cliente_id) as quantidade_de_fraudes
    FROM fraudulent_transactions ft 
    JOIN clientes c 
    ON c.id = ft.cliente_id 
    GROUP BY c.estado;

SELECT * 
FROM frauds_by_state
ORDER BY quantidade_de_fraudes DESC;

CREATE VIEW sum_of_frauds AS
    SELECT tipo_transacao, SUM(valor) as somatorio_valor
    FROM  fraudulent_transactions
    GROUP BY tipo_transacao;

SELECT * 
FROM sum_of_frauds
ORDER BY somatorio_valor DESC;

CREATE VIEW count_of_frauds AS
    SELECT tipo_transacao, COUNT(valor) as quantidade_transacoes
    FROM  fraudulent_transactions
    GROUP BY tipo_transacao;

SELECT * 
FROM count_of_frauds
ORDER BY quantidade_transacoes DESC;

CREATE VIEW frauds_by_year AS
    SELECT YEAR(data_hora) AS Ano, COUNT(*) AS frauds_by_year
    FROM fraudulent_transactions
    GROUP BY YEAR(data_hora);

SELECT * 
FROM frauds_by_year
ORDER BY Ano DESC;

CREATE VIEW frauds_by_shift_and_year AS
    SELECT DATEPART(year, data_hora) AS ano, 
        CASE 
            WHEN DATEPART(hour, data_hora) BETWEEN 0 AND 5 THEN '0-6 horas'
            WHEN DATEPART(hour, data_hora) BETWEEN 6 AND 11 THEN '6-12 horas'
            WHEN DATEPART(hour, data_hora) BETWEEN 12 AND 17 THEN '12-18 horas'
            WHEN DATEPART(hour, data_hora) BETWEEN 18 AND 23 THEN '18-24 horas'
        END AS horario_transacao,
        COUNT(*) AS quantidade_transacoes
    FROM fraudulent_transactions
    GROUP BY DATEPART(year, data_hora), 
            CASE 
                WHEN DATEPART(hour, data_hora) BETWEEN 0 AND 5 THEN '0-6 horas'
                WHEN DATEPART(hour, data_hora) BETWEEN 6 AND 11 THEN '6-12 horas'
                WHEN DATEPART(hour, data_hora) BETWEEN 12 AND 17 THEN '12-18 horas'
                WHEN DATEPART(hour, data_hora) BETWEEN 18 AND 23 THEN '18-24 horas'
            END;

SELECT * 
FROM frauds_by_shift_and_year
ORDER BY ano;

CREATE VIEW frauds_by_month AS
    SELECT MONTH(data_hora) AS mes, COUNT(id_transaction) AS quantidade_de_fraudes
    FROM fraudulent_transactions
    GROUP BY MONTH(data_hora);

SELECT * 
FROM frauds_by_month
ORDER BY quantidade_de_fraudes DESC;
