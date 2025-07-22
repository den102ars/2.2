CREATE TABLE dm.dm_f101_round_f (
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    chapter CHAR(1),
    ledger_account CHAR(5) NOT NULL,
    characteristic CHAR(1) NOT NULL,
    balance_in_rub NUMERIC(23,8),
    balance_in_val NUMERIC(23,8),
    balance_in_total NUMERIC(23,8),
    turn_deb_rub NUMERIC(23,8),
    turn_deb_val NUMERIC(23,8),
    turn_deb_total NUMERIC(23,8),
    turn_cre_rub NUMERIC(23,8),
    turn_cre_val NUMERIC(23,8),
    turn_cre_total NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    balance_out_val NUMERIC(23,8),
    balance_out_total NUMERIC(23,8),
    CONSTRAINT dm_f101_round_f_pkey PRIMARY KEY (from_date, ledger_account)
);

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_process_name TEXT := 'fill_f101_round_f';
    v_start_time TIMESTAMP := clock_timestamp();
    v_end_time TIMESTAMP;
    v_period_start DATE;
    v_period_end DATE;
    v_rows_affected INT;
    v_message TEXT;
BEGIN
    -- Логирование старта
    INSERT INTO logs.etl_log (process_name, start_time, status, message)
    VALUES (v_process_name, v_start_time, 'STARTED', 
            'Расчет 101-й формы за ' || i_OnDate::TEXT);

    -- Расчет периода
    v_period_start = (DATE_TRUNC('month', i_OnDate - INTERVAL '1 month'))::DATE;
    v_period_end = (i_OnDate - INTERVAL '1 day')::DATE;

    -- Удаление старых данных
    DELETE FROM dm.dm_f101_round_f 
    WHERE from_date = v_period_start AND to_date = v_period_end;

    -- Вставка новых данных
    INSERT INTO dm.dm_f101_round_f
    WITH period AS (
        SELECT 
            v_period_start AS start_date, 
            v_period_end AS end_date,
            (v_period_start - INTERVAL '1 day')::DATE AS prev_date
    ),
    accounts AS (
        SELECT DISTINCT account_rk
        FROM dm.dm_account_balance_f
        WHERE on_date BETWEEN (SELECT start_date FROM period) AND (SELECT end_date FROM period)
        UNION
        SELECT DISTINCT account_rk
        FROM dm.dm_account_turnover_f
        WHERE on_date BETWEEN (SELECT start_date FROM period) AND (SELECT end_date FROM period)
    ),
    account_info AS (
        SELECT 
            a.account_rk,
            a.account_number,
            a.char_type,
            a.currency_code::TEXT
        FROM accounts
        JOIN ds.md_account_d a ON a.account_rk = accounts.account_rk
        WHERE a.data_actual_date <= (SELECT end_date FROM period)
          AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date > (SELECT end_date FROM period))
    ),
    balance_in AS (
        SELECT 
            b.account_rk,
            b.balance_out_rub
        FROM dm.dm_account_balance_f b
        WHERE b.on_date = (SELECT prev_date FROM period)
    ),
    balance_out AS (
        SELECT 
            b.account_rk,
            b.balance_out_rub
        FROM dm.dm_account_balance_f b
        WHERE b.on_date = (SELECT end_date FROM period)
    ),
    turnover AS (
        SELECT 
            t.account_rk,
            SUM(t.debet_amount_rub) AS debet_amount_rub,
            SUM(t.credit_amount_rub) AS credit_amount_rub
        FROM dm.dm_account_turnover_f t
        WHERE t.on_date BETWEEN (SELECT start_date FROM period) AND (SELECT end_date FROM period)
        GROUP BY t.account_rk
    ),
    account_data AS (
        SELECT 
            ai.account_rk,
            LEFT(ai.account_number, 5) AS ledger_account,
            ai.char_type,
            ai.currency_code,
            COALESCE(bi.balance_out_rub, 0) AS balance_in,
            COALESCE(bo.balance_out_rub, 0) AS balance_out,
            COALESCE(t.debet_amount_rub, 0) AS debet_turn,
            COALESCE(t.credit_amount_rub, 0) AS credit_turn
        FROM account_info ai
        LEFT JOIN balance_in bi ON bi.account_rk = ai.account_rk
        LEFT JOIN balance_out bo ON bo.account_rk = ai.account_rk
        LEFT JOIN turnover t ON t.account_rk = ai.account_rk
    ),
    ledger_info AS (
        SELECT 
            l.ledger_account::TEXT,
            l.chapter
        FROM ds.md_ledger_account_s l
        WHERE l.start_date <= (SELECT end_date FROM period)
          AND (l.end_date IS NULL OR l.end_date >= (SELECT end_date FROM period))
    )
    SELECT 
        v_period_start,
        v_period_end,
        li.chapter::CHAR(1),
        ad.ledger_account::CHAR(5),
        ad.char_type::CHAR(1),
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN ad.balance_in ELSE 0 END),
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN ad.balance_in ELSE 0 END),
        SUM(ad.balance_in),
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN ad.debet_turn ELSE 0 END),
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN ad.debet_turn ELSE 0 END),
        SUM(ad.debet_turn),
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN ad.credit_turn ELSE 0 END),
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN ad.credit_turn ELSE 0 END),
        SUM(ad.credit_turn),
        SUM(CASE WHEN ad.currency_code IN ('810', '643') THEN ad.balance_out ELSE 0 END),
        SUM(CASE WHEN ad.currency_code NOT IN ('810', '643') THEN ad.balance_out ELSE 0 END),
        SUM(ad.balance_out)
    FROM account_data ad
    LEFT JOIN ledger_info li ON li.ledger_account = ad.ledger_account
    GROUP BY li.chapter, ad.ledger_account, ad.char_type;

    -- Логирование успеха
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    v_end_time := clock_timestamp();
    
    INSERT INTO logs.etl_log (process_name, start_time, end_time, status, rows_loaded, message)
    VALUES (v_process_name, v_start_time, v_end_time, 'COMPLETED', v_rows_affected, 
            'Успешно. Рассчитано строк: ' || v_rows_affected);

EXCEPTION
    WHEN OTHERS THEN
        v_end_time := clock_timestamp();
        v_message = 'Ошибка: ' || SQLERRM;
        
        INSERT INTO logs.etl_log (process_name, start_time, end_time, status, message)
        VALUES (v_process_name, v_start_time, v_end_time, 'ERROR', v_message);
        RAISE;
END;
$$;

CALL dm.fill_f101_round_f('2018-02-01');