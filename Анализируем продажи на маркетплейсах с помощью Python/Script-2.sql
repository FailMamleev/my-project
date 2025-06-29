- loan_term - Срок займа в днях.
- min_loan_term - Минимальный срок предыдущих займов.
- max_loan_term - Максимальный срок предыдущих займов.
- is_closed - Является ли контракт закрытым на текущий момент. Закрытым считаем контракт со следующими статусами: «Закрыт», «Договор закрыт с переплатой», «Переоформлен».
- usage_days - Количество дней фактического использования займа (для закрытых). Разница между датой закрытия и открытия займа.
- dev_days - Разница между датой закрытия и последней датой планового погашения (для закрытых).
- delay_days - Количество дней с даты закрытия предыдущего займа у клиента и датой открытия текущего.

with plc as
(select condition_id
		, count(*)
from test_data_contract_conditions_payment_plan
group by condition_id
having count(*) > 1)

select t1.contract_id
	, t1.contract_code
	, t1.customer_id
	, t2.condition_id
	, t1.subdivision_id
	, dense_rank () over (partition by t1.customer_id order by t1.issue_dt) as contract_serial_number
	, case
		when t1.renewal_contract_id is null then
			dense_rank () over (partition by t1.customer_id, t1.renewal_contract_id order by t1.issue_dt) end as contract_renewal_serial_number
	, case
		when t1.renewal_contract_id is null then 0 else 1 end as is_renewal
	, case
		when plc.condition_id is not null then 1 else 0 end as is_installment
	, COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count
	, min(t1.issue_dt) over (partition by t1.customer_id) as first_issue_dt
	, t1.issue_dt
	, MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt
	, MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt
	, (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t4.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt
	, t3.loan_amount
	, sum(t3.loan_amount) over (partition by t1.customer_id) as total_loan_amount
	, min(t3.loan_amount) over (partition by t1.customer_id) as min_loan_amount
	, max(t3.loan_amount) over (partition by t1.customer_id) as max_loan_amount
	, SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term
	, MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term
	, MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term
	, CASE
      WHEN (SELECT MAX(t4.status_dt)
            FROM test_data_contract_status t4
            WHERE t4.contract_id = t1.contract_id
              AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN TRUE
      ELSE FALSE
    END AS is_closed
    , CASE WHEN (SELECT COUNT(*) 
              FROM test_data_contract t1_next
              WHERE t1_next.customer_id = t1.customer_id 
                AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 ELSE 0 END AS has_next
from test_data_contract t1
	LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
	LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
	LEFT JOIN plc ON plc.condition_id = t2.condition_id



	
COMMENT ON TABLE test_data_contract IS 'Общая информация о контрактах'
COMMENT ON TABLE test_data_contract_conditions IS 'Информация об изменении условий контракта, в том числе перенос срока и продление'
COMMENT ON TABLE test_data_contract_conditions_payment_plan IS 'План-график выплат по контракту'
COMMENT ON TABLE test_data_contract_status IS 'Информация об изменении статуса контракта'
	
COMMENT ON COLUMN test_data_contract.customer_id IS 'ID клиента'	
COMMENT ON COLUMN test_data_contract.contract_id IS 'ID контракта'	
COMMENT ON COLUMN test_data_contract.contract_code IS 'код контракта в CRM'	
COMMENT ON COLUMN test_data_contract.marker_delete IS 'пометка на удаление'	
COMMENT ON COLUMN test_data_contract.issue_dt IS 'дата заключения контракта'	
COMMENT ON COLUMN test_data_contract.subdivision_id IS 'ID подразделения, заключившего контракт'	
COMMENT ON COLUMN test_data_contract.renewal_contract_id IS 'ID контракта, который стал причиной переоформления'	

COMMENT ON COLUMN test_data_contract_conditions.condition_id IS 'ID изменения условий контракта'	
COMMENT ON COLUMN test_data_contract_conditions.condition_dt IS 'Дата изменения условия'	
COMMENT ON COLUMN test_data_contract_conditions.contract_id IS 'ID контракта'	
COMMENT ON COLUMN test_data_contract_conditions.conducted IS 'Признак проведения изменения в CRM'
COMMENT ON COLUMN test_data_contract_conditions.marker_delete IS 'Пометка на удаление'	
COMMENT ON COLUMN test_data_contract_conditions.operation_type IS 'Тип операции'	
COMMENT ON COLUMN test_data_contract_conditions.condition_type IS 'Тип изменения условий'	
COMMENT ON COLUMN test_data_contract_conditions.condition_start_dt IS 'Дата начала действия изменения условий. Относится к сроку действия контракта или продления.'	
COMMENT ON COLUMN test_data_contract_conditions.condition_end_dt IS 'Дата окончания действия изменения условий. Относится к сроку действия контракта или продления'	
COMMENT ON COLUMN test_data_contract_conditions.days IS 'Срок действия изменения условий в днях'	

COMMENT ON COLUMN test_data_contract_conditions_payment_plan.condition_id IS 'ID изменения условий контракта'	
COMMENT ON COLUMN test_data_contract_conditions_payment_plan.payment_dt IS 'Дата платежа'	
COMMENT ON COLUMN test_data_contract_conditions_payment_plan.loan_amount IS 'Сумма платежа'	

COMMENT ON COLUMN test_data_contract_status.contract_id IS 'ID контракта'
COMMENT ON COLUMN test_data_contract_status.status_dt IS 'Дата и время статуса контракта'	
COMMENT ON COLUMN test_data_contract_status.status_type IS 'Вид статуса контракта'	

COMMENT ON COLUMN test_data_contract_status.contract_serial_number IS 'Порядковый номер контракта у клиента'
COMMENT ON COLUMN test_data_contract_status.contract_renewal_serial_number IS 'Порядковый номер контракта у клиента без учёта переоформлений. Если контракт является переоформлением, порядковый номер не должен увеличиваться'
COMMENT ON COLUMN test_data_contract_status.is_renewal IS 'Является ли данный контракт переоформлением (наличие ID в поле renewal_contract_id)'
COMMENT ON COLUMN test_data_contract_status.is_installment IS 'Является ли данный контракт долгосрочным (наличие нескольких платежей в плане погашений).'
COMMENT ON COLUMN test_data_contract_status.prolong_count IS 'Количество продлений. Контракт может быть продлён неограниченное количество раз (см. test_data_contract_conditions.condition_type)'
COMMENT ON COLUMN test_data_contract_status.first_issue_dt IS 'Дата первого контракта у клиента'
COMMENT ON COLUMN test_data_contract_status.plan_dt IS 'Дата планового погашения займа'
COMMENT ON COLUMN test_data_contract_status.last_plan_dt IS 'Дата планового погашения займа с учётом продлений'
COMMENT ON COLUMN test_data_contract_status.close_dt IS 'Дата фактического погашения займа (дата закрытия). Учитывается только последний статус по контракту'
COMMENT ON COLUMN test_data_contract_status.loan_amount IS 'Сумма займа. Суммируются все платежи по графику'
COMMENT ON COLUMN test_data_contract_status.total_loan_amount IS 'Сумма всех предыдущих займов'
COMMENT ON COLUMN test_data_contract_status.min_loan_amount IS 'Минимальная сумма предыдущих займов'
COMMENT ON COLUMN test_data_contract_status.max_loan_amount IS 'Максимальная сумма предыдущих займов'
COMMENT ON COLUMN test_data_contract_status.loan_term IS 'Срок займа в днях'
COMMENT ON COLUMN test_data_contract_status.min_loan_term IS 'Минимальный срок предыдущих займов'
COMMENT ON COLUMN test_data_contract_status.max_loan_term IS 'Максимальный срок предыдущих займов'
COMMENT ON COLUMN test_data_contract_status.is_closed IS 'Является ли контракт закрытым на текущий момент. Закрытым считаем контракт со следующими статусами: «Закрыт», «Договор закрыт с переплатой», «Переоформлен»'
COMMENT ON COLUMN test_data_contract_status.usage_days IS 'Количество дней фактического использования займа (для закрытых). Разница между датой закрытия и открытия займа'
COMMENT ON COLUMN test_data_contract_status.dev_days IS 'Разница между датой закрытия и последней датой планового погашения (для закрытых).'
COMMENT ON COLUMN test_data_contract_status.delay_days IS 'Количество дней с даты закрытия предыдущего займа у клиента и датой открытия текущего'
COMMENT ON COLUMN test_data_contract_status.has_next IS 'Признак наличия следующего контракта у клиента'

CREATE INDEX contract_id ON table_name (contract_id)
CREATE INDEX customer_id ON table_name (customer_id)
CREATE INDEX condition_id ON table_name (condition_id)








	
	
	
WITH plc AS (
  SELECT condition_id, COUNT(*) AS cnt
  FROM test_data_contract_conditions_payment_plan
  GROUP BY condition_id
  HAVING COUNT(*) > 1
),
v_contract AS (
  SELECT
    t1.contract_id,
    t1.contract_code,
    t1.customer_id,
    t2.condition_id,
    t1.subdivision_id,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = t1.contract_id
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
    t1.issue_dt,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = (CASE WHEN t1.renewal_contract_id IS NULL THEN t1.contract_id ELSE t1.renewal_contract_id END)
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS prev_close_dt
  FROM test_data_contract t1
  LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
  LEFT JOIN plc ON plc.condition_id = t2.condition_id
),
usage_days AS (
  SELECT
    contract_id,
    CASE
      WHEN close_dt IS NOT NULL THEN close_dt - issue_dt
      ELSE NULL
    END AS usage_days
  FROM v_contract
),
dev_days AS (
  SELECT
    contract_id,
    CASE
      WHEN close_dt IS NOT NULL THEN close_dt - MAX(last_plan_dt)
      ELSE NULL
    END AS dev_days
  FROM v_contract
  LEFT JOIN (
    SELECT
      condition_id,
      MAX(payment_dt) AS last_plan_dt
    FROM test_data_contract_conditions_payment_plan
    GROUP BY condition_id
  ) AS payment_plan
  ON v_contract.condition_id = payment_plan.condition_id
  GROUP BY contract_id, close_dt
),
delay_days AS (
  SELECT
    contract_id,
    CASE
      WHEN prev_close_dt IS NOT NULL THEN issue_dt - prev_close_dt
      ELSE NULL
    END AS delay_days
  FROM v_contract
)
SELECT
  usage_days.usage_days,
  dev_days.dev_days,
  delay_days.delay_days
FROM usage_days
LEFT JOIN dev_days USING (contract_id)
LEFT JOIN delay_days USING (contract_id)

        
-- Этот код берет данные из подготовленной таблицы contract_info и вычисляет требуемые поля:

-- usage_days - разница между датой закрытия контракта и датой его открытия
-- dev_days - разница между датой закрытия контракта и последней датой планового погашения
-- delay_days - разница между датой открытия текущего контракта и датой закрытия предыдущего контракта клиента

-- Результат запроса содержит только эти 3 вычисленных поля.
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
WITH plc AS (
  SELECT condition_id, COUNT(*) AS cnt
  FROM test_data_contract_conditions_payment_plan
  GROUP BY condition_id
  HAVING COUNT(*) > 1
),
contract_info AS (
select t1.contract_id
	, t1.contract_code
	, t1.customer_id
	, t2.condition_id
	, t1.subdivision_id
	, dense_rank () over (partition by t1.customer_id order by t1.issue_dt) as contract_serial_number
	, case
		when t1.renewal_contract_id is null then
			dense_rank () over (partition by t1.customer_id, t1.renewal_contract_id order by t1.issue_dt) end as contract_renewal_serial_number
	, case
		when t1.renewal_contract_id is null then 0 else 1 end as is_renewal
	, case
		when plc.condition_id is not null then 1 else 0 end as is_installment
	, COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count
	, min(t1.issue_dt) over (partition by t1.customer_id) as first_issue_dt
	, t1.issue_dt
	, MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt
	, MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt
	, (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t4.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt
	, t3.loan_amount
	, sum(t3.loan_amount) over (partition by t1.customer_id) as total_loan_amount
	, min(t3.loan_amount) over (partition by t1.customer_id) as min_loan_amount
	, max(t3.loan_amount) over (partition by t1.customer_id) as max_loan_amount
	, SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term
	, MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term
	, MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term
	, CASE
    WHEN (SELECT MAX(t4.status_dt)
          FROM test_data_contract_status t4
          WHERE t4.contract_id = (CASE WHEN t1.renewal_contract_id IS NULL THEN t1.contract_id ELSE t1.renewal_contract_id END)
            AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN
        (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = (CASE WHEN t1.renewal_contract_id IS NULL THEN t1.contract_id ELSE t1.renewal_contract_id END)
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) ELSE null END AS prev_close_dt
    , CASE WHEN (SELECT COUNT(*) 
              FROM test_data_contract t1_next
              WHERE t1_next.customer_id = t1.customer_id 
                AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 ELSE 0 END AS has_next
	, CASE
      WHEN close_dt IS NOT NULL THEN close_dt - issue_dt ELSE null END AS usage_days
    , CASE
      WHEN close_dt IS NOT NULL THEN close_dt - MAX(last_plan_dt) ELSE null END AS dev_days
    , CASE
      WHEN prev_close_dt IS NOT NULL THEN issue_dt - prev_close_dt ELSE null END AS delay_days
from test_data_contract t1
	LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
	LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
	LEFT JOIN plc ON plc.condition_id = t2.condition_id



WITH plc AS (
  SELECT condition_id, COUNT(*) AS cnt
  FROM test_data_contract_conditions_payment_plan
  GROUP BY condition_id
  HAVING COUNT(*) > 1
),
contract_info AS (
  SELECT
    t1.contract_id,
    t1.contract_code,
    t1.customer_id,
    t2.condition_id,
    t1.subdivision_id,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = t1.contract_id
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
    t1.issue_dt,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = (CASE WHEN t1.renewal_contract_id IS NULL THEN t1.contract_id ELSE t1.renewal_contract_id END)
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS prev_close_dt
  FROM test_data_contract t1
  LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
  LEFT JOIN plc ON plc.condition_id = t2.condition_id
),
usage_days AS (
  SELECT
    contract_id,
    CASE
      WHEN close_dt IS NOT NULL THEN close_dt - issue_dt
      ELSE NULL
    END AS usage_days
  FROM contract_info
),
dev_days AS (
  SELECT
    contract_id,
    CASE
      WHEN close_dt IS NOT NULL THEN close_dt - MAX(last_plan_dt)
      ELSE NULL
    END AS dev_days
  FROM contract_info
  LEFT JOIN (
    SELECT
      condition_id,
      MAX(payment_dt) AS last_plan_dt
    FROM test_data_contract_conditions_payment_plan
    GROUP BY condition_id
  ) AS payment_plan
  ON contract_info.condition_id = payment_plan.condition_id
  GROUP BY contract_id, close_dt
),
delay_days AS (
  SELECT
    contract_id,
    CASE
      WHEN prev_close_dt IS NOT NULL THEN issue_dt - prev_close_dt
      ELSE NULL
    END AS delay_days
  FROM contract_info
)
select usage_days.usage_days
	, dev_days.dev_days
	, delay_days.delay_days
FROM usage_days
LEFT JOIN dev_days USING (contract_id)
LEFT JOIN delay_days USING (contract_id)









with plc as
(select condition_id
		, count(*)
from test_data_contract_conditions_payment_plan
group by condition_id
having count(*) > 1)

select t1.contract_id
	, t1.contract_code
	, t1.customer_id
	, t2.condition_id
	, t1.subdivision_id
	, dense_rank () over (partition by t1.customer_id order by t1.issue_dt) as contract_serial_number
	, case
		when t1.renewal_contract_id is null then
			dense_rank () over (partition by t1.customer_id, t1.renewal_contract_id order by t1.issue_dt) end as contract_renewal_serial_number
	, case
		when t1.renewal_contract_id is null then 0 else 1 end as is_renewal
	, case
		when plc.condition_id is not null then 1 else 0 end as is_installment
	, COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count
	, min(t1.issue_dt) over (partition by t1.customer_id) as first_issue_dt
	, t1.issue_dt
	, MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt
	, MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt
	, (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t4.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt
	, t3.loan_amount
	, sum(t3.loan_amount) over (partition by t1.customer_id) as total_loan_amount
	, min(t3.loan_amount) over (partition by t1.customer_id) as min_loan_amount
	, max(t3.loan_amount) over (partition by t1.customer_id) as max_loan_amount
	, SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term
	, MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term
	, MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term
	, CASE
      WHEN (SELECT MAX(t4.status_dt)
            FROM test_data_contract_status t4
            WHERE t4.contract_id = t1.contract_id
              AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN TRUE
      ELSE FALSE
    END AS is_closed
    , CASE WHEN (SELECT COUNT(*) 
              FROM test_data_contract t1_next
              WHERE t1_next.customer_id = t1.customer_id 
                AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 ELSE 0 END AS has_next
	, CASE
      WHEN close_dt IS NOT NULL THEN close_dt - issue_dt ELSE null END AS usage_days
    , CASE
      WHEN close_dt IS NOT NULL THEN close_dt - MAX(last_plan_dt) ELSE null END AS dev_days
    , CASE
      WHEN is_closed IS NOT NULL THEN issue_dt - is_closed ELSE null END AS delay_days
from test_data_contract t1
	LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
	LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
	LEFT JOIN plc ON plc.condition_id = t2.condition_id

	
	
	
	
	
	
	
	
	
	
WITH plc AS (
  SELECT condition_id, COUNT(*) AS cnt
  FROM test_data_contract_conditions_payment_plan
  GROUP BY condition_id
  HAVING COUNT(*) > 1
)
  SELECT
    t1.contract_id,
    t1.contract_code,
    t1.customer_id,
    t2.condition_id,
    t1.subdivision_id,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = t1.contract_id
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
    t1.issue_dt,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = (CASE WHEN t1.renewal_contract_id IS NULL THEN t1.contract_id ELSE t1.renewal_contract_id END)
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS prev_close_dt
   
       
       
	, dense_rank () over (partition by t1.customer_id order by t1.issue_dt) as contract_serial_number
	, case
		when t1.renewal_contract_id is null then
			dense_rank () over (partition by t1.customer_id, t1.renewal_contract_id order by t1.issue_dt) end as contract_renewal_serial_number
	, case
		when t1.renewal_contract_id is null then 0 else 1 end as is_renewal
	, case
		when plc.condition_id is not null then 1 else 0 end as is_installment
	, COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count
	, min(t1.issue_dt) over (partition by t1.customer_id) as first_issue_dt       
	, MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt
	, MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt
    , CASE WHEN (SELECT COUNT(*) 
              FROM test_data_contract t1_next
              WHERE t1_next.customer_id = t1.customer_id 
                AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 ELSE 0 END AS has_next       
	, t3.loan_amount
	, sum(t3.loan_amount) over (partition by t1.customer_id) as total_loan_amount
	, min(t3.loan_amount) over (partition by t1.customer_id) as min_loan_amount
	, max(t3.loan_amount) over (partition by t1.customer_id) as max_loan_amount
	, SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term
	, MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term
	, MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term       

	
	, MAX(CASE WHEN t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен') THEN (MAX(t4.status_dt) - MIN(t1.issue_dt)) ELSE NULL END) AS usage_days
    , MAX(CASE WHEN t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен') THEN (MAX(t4.status_dt) - MAX(t3.payment_dt)) ELSE NULL END) AS dev_days
    , MAX(CASE WHEN t1.renewal_contract_id IS NOT NULL THEN (t1.issue_dt - (SELECT MAX(t4.status_dt) FROM test_data_contract_status WHERE contract_id = t1.renewal_contract_id)) ELSE NULL END) AS delay_days,	
	
	
	
       
from test_data_contract t1
	LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
	LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
	LEFT JOIN plc ON plc.condition_id = t2.condition_id
	LEFT JOIN test_data_contract_status t4 ON t4.contract_id = t1.contract_id
	

	
	
	
	
	
	
	
	
	
	
	
	WITH plc AS (
  SELECT condition_id, COUNT(*) AS cnt
  FROM test_data_contract_conditions_payment_plan
  GROUP BY condition_id
  HAVING COUNT(*) > 1
),
closed_contracts AS (
  SELECT
    t1.contract_id,
    t1.contract_code,
    t1.customer_id,
    t2.condition_id,
    t1.subdivision_id,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = t1.contract_id
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
    t1.issue_dt,
    (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4
     WHERE t4.contract_id = (CASE WHEN t1.renewal_contract_id IS NULL THEN t1.contract_id ELSE t1.renewal_contract_id END)
       AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS prev_close_dt,
    DATEDIFF(
      (SELECT MAX(t4.status_dt)
       FROM test_data_contract_status t4
       WHERE t4.contract_id = t1.contract_id
         AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')),
      t1.issue_dt
    ) AS usage_days,
    DATEDIFF(
      (SELECT MAX(t4.status_dt)
       FROM test_data_contract_status t4
       WHERE t4.contract_id = t1.contract_id
         AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')),
      (SELECT MAX(t3.payment_dt)
       FROM test_data_contract_conditions_payment_plan t3
       WHERE t3.condition_id = t2.condition_id)
    ) AS dev_days,
    DATEDIFF(
      t1.issue_dt,
      (SELECT MAX(t4.status_dt)
       FROM test_data_contract_status t4
       WHERE t4.contract_id = (CASE WHEN t1.renewal_contract_id IS NULL THEN t1.contract_id ELSE t1.renewal_contract_id END)
         AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен'))
    ) AS delay_days
  FROM test_data_contract t1
  LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
  LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
  LEFT JOIN plc ON plc.condition_id = t2.condition_id
  WHERE (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t1.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
),
full_data AS (
  SELECT
    t1.contract_id,
    t1.contract_code,
    t1.customer_id,
    t2.condition_id,
    t1.subdivision_id,
    ROW_NUMBER() OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS contract_serial_number,
    ROW_NUMBER() OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt, CASE WHEN t1.renewal_contract_id IS NOT NULL THEN 1 ELSE 0 END) AS contract_renewal_serial_number,
  CASE WHEN t1.renewal_contract_id IS NOT NULL THEN 1 ELSE 0 END AS is_renewal,
  CASE WHEN plc.cnt > 1 THEN 1 ELSE 0 END AS is_installment,
  plc.cnt AS prolong_count,
  (SELECT MIN(t4.issue_dt)
   FROM test_data_contract t4
   WHERE t4.customer_id = t1.customer_id) AS first_issue_dt,
  t1.issue_dt,
  (SELECT MIN(t3.payment_dt)
   FROM test_data_contract_conditions_payment_plan t3
   WHERE t3.condition_id = t2.condition_id) AS plan_dt,
  (SELECT MAX(t3.payment_dt)
   FROM test_data_contract_conditions_payment_plan t3
   WHERE t3.condition_id = t2.condition_id) AS last_plan_dt,
  (SELECT MAX(t4.status_dt)
   FROM test_data_contract_status t4
   WHERE t4.contract_id = t1.contract_id
     AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
  (SELECT SUM(t3.loan_amount)
   FROM test_data_contract_conditions_payment_plan t3
   WHERE t3.condition_id = t2.condition_id) AS loan_amount,
  (SELECT SUM(t4.loan_amount)
   FROM test_data_contract_conditions_payment_plan t4
   WHERE t4.condition_id IN (
     SELECT condition_id
     FROM test_data_contract_conditions
     WHERE contract_id IN (
       SELECT contract_id
       FROM test_data_contract
       WHERE customer_id = t1.customer_id
         AND contract_id <> t1.contract_id
     )
   )) AS total_loan_amount,
  (SELECT MIN(t4.loan_amount)
   FROM test_data_contract_conditions_payment_plan t4
   WHERE t4.condition_id IN (
     SELECT condition_id
     FROM test_data_contract_conditions
     WHERE contract_id IN (
       SELECT contract_id
       FROM test_data_contract
       WHERE customer_id = t1.customer_id
         AND contract_id <> t1.contract_id
     )
   )) AS min_loan_amount,
  (SELECT MAX(t4.loan_amount)
   FROM test_data_contract_conditions_payment_plan t4
   WHERE t4.condition_id IN (
     SELECT condition_id
     FROM test_data_contract_conditions
     WHERE contract_id IN (
       SELECT contract_id
       FROM test_data_contract
       WHERE customer_id = t1.customer_id
         AND contract_id <> t1.contract_id
     )
   )) AS max_loan_amount,
  (SELECT SUM(t3.days)
   FROM test_data_contract_conditions t3
   WHERE t3.contract_id = t1.contract_id) AS loan_term,
  (SELECT MIN(t4.days)
   FROM test_data_contract_conditions t4
   WHERE t4.contract_id IN (
     SELECT contract_id
     FROM test_data_contract
     WHERE customer_id = t1.customer_id
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     

     
     
     
     
     
     
     
     
     
     
     
with plc as
(select condition_id
		, count(*)
from test_data_contract_conditions_payment_plan
group by condition_id
having count(*) > 1)

select t1.contract_id
	, t1.contract_code
	, t1.customer_id
	, t2.condition_id
	, t1.subdivision_id
	, dense_rank () over (partition by t1.customer_id order by t1.issue_dt) as contract_serial_number
	, case
		when t1.renewal_contract_id is null then
			dense_rank () over (partition by t1.customer_id, t1.renewal_contract_id order by t1.issue_dt) end as contract_renewal_serial_number
	, case
		when t1.renewal_contract_id is null then 0 else 1 end as is_renewal
	, case
		when plc.condition_id is not null then 1 else 0 end as is_installment
	, COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count
	, min(t1.issue_dt) over (partition by t1.customer_id) as first_issue_dt
	, t1.issue_dt
	, MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt
	, MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt
	, (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t4.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt
	, t3.loan_amount
	, sum(t3.loan_amount) over (partition by t1.customer_id) as total_loan_amount
	, min(t3.loan_amount) over (partition by t1.customer_id) as min_loan_amount
	, max(t3.loan_amount) over (partition by t1.customer_id) as max_loan_amount
	, SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term
	, MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term
	, MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term
	, CASE
      WHEN (SELECT MAX(t4.status_dt)
            FROM test_data_contract_status t4
            WHERE t4.contract_id = t1.contract_id
              AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN TRUE
      ELSE FALSE
    END AS is_closed
    
     , CASE WHEN (SELECT MAX(t4.status_dt) 
               FROM test_data_contract_status t4 
               WHERE t4.contract_id = t1.contract_id 
                 AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
        THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) - t1.issue_dt 
     END AS usage_days
  , CASE WHEN (SELECT MAX(t4.status_dt) 
               FROM test_data_contract_status t4 
               WHERE t4.contract_id = t1.contract_id 
                 AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
        THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) 
             - (SELECT MAX(payment_dt) 
                FROM test_data_contract_conditions_payment_plan 
                WHERE condition_id = t2.condition_id) 
     END AS dev_days
  , t1.issue_dt - LAG(
      (SELECT MAX(t4.status_dt) 
       FROM test_data_contract_status t4 
       WHERE t4.contract_id = t1.contract_id  -- Исправлено: брать предыдущий contract_id через LAG
         AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
      )) OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS delay_days
  , CASE WHEN (SELECT COUNT(*) 
             FROM test_data_contract t1_next
             WHERE t1_next.customer_id = t1.customer_id 
               AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 ELSE 0 END AS has_next
from test_data_contract t1
LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
LEFT JOIN plc ON plc.condition_id = t2.condition_id









- 1
WITH plc AS (
    SELECT 
        condition_id, 
        COUNT(*) 
    FROM test_data_contract_conditions_payment_plan
    GROUP BY condition_id
    HAVING COUNT(*) > 1
), base_data AS (
    SELECT 
        t1.contract_id,
        t1.contract_code,
        t1.customer_id,
        t2.condition_id,
        t1.subdivision_id,
        DENSE_RANK() OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS contract_serial_number,
        CASE
            WHEN t1.renewal_contract_id IS NULL THEN
                DENSE_RANK() OVER (PARTITION BY t1.customer_id, t1.renewal_contract_id ORDER BY t1.issue_dt)
        END AS contract_renewal_serial_number,
        CASE WHEN t1.renewal_contract_id IS NULL THEN 0 ELSE 1 END AS is_renewal,
        CASE WHEN plc.condition_id IS NOT NULL THEN 1 ELSE 0 END AS is_installment,
        COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) 
            OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count,
        MIN(t1.issue_dt) OVER (PARTITION BY t1.customer_id) AS first_issue_dt,
        t1.issue_dt,
        MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt,
        MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt,
        (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t1.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
        t3.loan_amount,
        SUM(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS total_loan_amount,
        MIN(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS min_loan_amount,
        MAX(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS max_loan_amount,
        SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term,
        MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term,
        MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term,
        CASE
            WHEN (SELECT MAX(t4.status_dt)
                  FROM test_data_contract_status t4
                  WHERE t4.contract_id = t1.contract_id
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_closed,
        CASE 
            WHEN (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) - t1.issue_dt 
        END AS usage_days,
        CASE 
            WHEN (SELECT MAX(t4.status_dt) 
                  FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) 
                 - (SELECT MAX(payment_dt) 
                    FROM test_data_contract_conditions_payment_plan 
                    WHERE condition_id = t2.condition_id) 
        END AS dev_days,
        t1.issue_dt - LAG(
            (SELECT MAX(t4.status_dt) 
             FROM test_data_contract_status t4 
             WHERE t4.contract_id = t1.contract_id 
               AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
            )) OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS delay_days,
        CASE 
            WHEN (SELECT COUNT(*) 
                  FROM test_data_contract t1_next
                  WHERE t1_next.customer_id = t1.customer_id 
                    AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 
            ELSE 0 
        END AS has_next
    FROM test_data_contract t1
    LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
    LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
    LEFT JOIN plc ON plc.condition_id = t2.condition_id
)
SELECT *
FROM base_data
WHERE EXTRACT(YEAR FROM first_issue_dt) = 2019;




- 2

WITH plc AS (
    SELECT 
        condition_id, 
        COUNT(*) 
    FROM test_data_contract_conditions_payment_plan
    GROUP BY condition_id
    HAVING COUNT(*) > 1
), base_data AS (
    SELECT 
        t1.contract_id,
        t1.contract_code,
        t1.customer_id,
        t2.condition_id,
        t1.subdivision_id,
        DENSE_RANK() OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS contract_serial_number,
        CASE
            WHEN t1.renewal_contract_id IS NULL THEN
                DENSE_RANK() OVER (PARTITION BY t1.customer_id, t1.renewal_contract_id ORDER BY t1.issue_dt)
        END AS contract_renewal_serial_number,
        CASE WHEN t1.renewal_contract_id IS NULL THEN 0 ELSE 1 END AS is_renewal,
        CASE WHEN plc.condition_id IS NOT NULL THEN 1 ELSE 0 END AS is_installment,
        COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) 
            OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count,
        MIN(t1.issue_dt) OVER (PARTITION BY t1.customer_id) AS first_issue_dt,
        t1.issue_dt,
        MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt,
        MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt,
        (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t1.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
        t3.loan_amount,
        SUM(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS total_loan_amount,
        MIN(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS min_loan_amount,
        MAX(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS max_loan_amount,
        SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term,
        MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term,
        MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term,
        CASE
            WHEN (SELECT MAX(t4.status_dt)
                  FROM test_data_contract_status t4
                  WHERE t4.contract_id = t1.contract_id
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_closed,
        CASE 
            WHEN (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) - t1.issue_dt 
        END AS usage_days,
        CASE 
            WHEN (SELECT MAX(t4.status_dt) 
                  FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) 
                 - (SELECT MAX(payment_dt) 
                    FROM test_data_contract_conditions_payment_plan 
                    WHERE condition_id = t2.condition_id) 
        END AS dev_days,
        t1.issue_dt - LAG(
            (SELECT MAX(t4.status_dt) 
             FROM test_data_contract_status t4 
             WHERE t4.contract_id = t1.contract_id 
               AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
            )) OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS delay_days,
        CASE 
            WHEN (SELECT COUNT(*) 
                  FROM test_data_contract t1_next
                  WHERE t1_next.customer_id = t1.customer_id 
                    AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 
            ELSE 0 
        END AS has_next
    FROM test_data_contract t1
    LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
    LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
    LEFT JOIN plc ON plc.condition_id = t2.condition_id
)
SELECT 
    EXTRACT(YEAR FROM first_issue_dt) AS start_year,  -- год первого договора
    EXTRACT(MONTH FROM first_issue_dt) AS start_month,  -- месяц первого договора
    COUNT(DISTINCT customer_id) AS clients_count  -- количество уникальных клиентов
FROM base_data
GROUP BY start_year, start_month
ORDER BY start_year, start_month;




- 3

WITH plc AS (
    SELECT 
        condition_id, 
        COUNT(*) 
    FROM test_data_contract_conditions_payment_plan
    GROUP BY condition_id
    HAVING COUNT(*) > 1
), base_data AS (
    SELECT 
        t1.contract_id,
        t1.contract_code,
        t1.customer_id,
        t2.condition_id,
        t1.subdivision_id,
        DENSE_RANK() OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS contract_serial_number,
        CASE
            WHEN t1.renewal_contract_id IS NULL THEN
                DENSE_RANK() OVER (PARTITION BY t1.customer_id, t1.renewal_contract_id ORDER BY t1.issue_dt)
        END AS contract_renewal_serial_number,
        CASE WHEN t1.renewal_contract_id IS NULL THEN 0 ELSE 1 END AS is_renewal,
        CASE WHEN plc.condition_id IS NOT NULL THEN 1 ELSE 0 END AS is_installment,
        COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) 
            OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count,
        MIN(t1.issue_dt) OVER (PARTITION BY t1.customer_id) AS first_issue_dt,
        t1.issue_dt,
        MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt,
        MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt,
        (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t1.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
        t3.loan_amount,
        SUM(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS total_loan_amount,
        MIN(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS min_loan_amount,
        MAX(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS max_loan_amount,
        SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term,
        MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term,
        MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term,
        CASE
            WHEN (SELECT MAX(t4.status_dt)
                  FROM test_data_contract_status t4
                  WHERE t4.contract_id = t1.contract_id
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_closed,
        CASE 
            WHEN (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) - t1.issue_dt 
        END AS usage_days,
        CASE 
            WHEN (SELECT MAX(t4.status_dt) 
                  FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) 
                 - (SELECT MAX(payment_dt) 
                    FROM test_data_contract_conditions_payment_plan 
                    WHERE condition_id = t2.condition_id) 
        END AS dev_days,
        t1.issue_dt - LAG(
            (SELECT MAX(t4.status_dt) 
             FROM test_data_contract_status t4 
             WHERE t4.contract_id = t1.contract_id 
               AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
            )) OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS delay_days,
        CASE 
            WHEN (SELECT COUNT(*) 
                  FROM test_data_contract t1_next
                  WHERE t1_next.customer_id = t1.customer_id 
                    AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 
            ELSE 0 
        END AS has_next
    FROM test_data_contract t1
    LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
    LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
    LEFT JOIN plc ON plc.condition_id = t2.condition_id
),
customer_groups AS (
    SELECT 
        customer_id,
        EXTRACT(YEAR FROM first_issue_dt) AS start_year,
        EXTRACT(MONTH FROM first_issue_dt) AS start_month,
        MAX(contract_serial_number) AS max_contract_number  -- Максимальный номер договора для клиента
    FROM base_data
    GROUP BY customer_id, start_year, start_month  -- Одна запись на клиента в группе
),
ranked_customers AS (
    SELECT 
        start_year,
        start_month,
        customer_id,
        max_contract_number,
        DENSE_RANK() OVER (
            PARTITION BY start_year, start_month 
            ORDER BY max_contract_number DESC
        ) AS rank_in_group  -- Ранжирование клиентов внутри группы по убыванию номера
    FROM customer_groups
)
SELECT 
    start_year,
    start_month,
    customer_id,
    max_contract_number AS max_serial
FROM ranked_customers
WHERE rank_in_group = 1  -- Выбор только клиентов с максимальным номером
ORDER BY start_year, start_month;



- 4


WITH plc AS (
    SELECT 
        condition_id, 
        COUNT(*) 
    FROM test_data_contract_conditions_payment_plan
    GROUP BY condition_id
    HAVING COUNT(*) > 1
), base_data AS (
    SELECT 
        t1.contract_id,
        t1.contract_code,
        t1.customer_id,
        t2.condition_id,
        t1.subdivision_id,
        DENSE_RANK() OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS contract_serial_number,
        CASE
            WHEN t1.renewal_contract_id IS NULL THEN
                DENSE_RANK() OVER (PARTITION BY t1.customer_id, t1.renewal_contract_id ORDER BY t1.issue_dt)
        END AS contract_renewal_serial_number,
        CASE WHEN t1.renewal_contract_id IS NULL THEN 0 ELSE 1 END AS is_renewal,
        CASE WHEN plc.condition_id IS NOT NULL THEN 1 ELSE 0 END AS is_installment,
        COUNT(CASE WHEN t2.condition_type LIKE '%Продление%' THEN 1 END) 
            OVER (PARTITION BY t1.contract_id ORDER BY t2.condition_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prolong_count,
        MIN(t1.issue_dt) OVER (PARTITION BY t1.customer_id) AS first_issue_dt,
        t1.issue_dt,
        MIN(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS plan_dt,
        MAX(t3.payment_dt) OVER (PARTITION BY t2.condition_id) AS last_plan_dt,
        (SELECT MAX(t4.status_dt)
         FROM test_data_contract_status t4
         WHERE t4.contract_id = t1.contract_id
           AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) AS close_dt,
        t3.loan_amount,
        SUM(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS total_loan_amount,
        MIN(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS min_loan_amount,
        MAX(t3.loan_amount) OVER (PARTITION BY t1.customer_id) AS max_loan_amount,
        SUM(t2.days) OVER (PARTITION BY t1.contract_id) AS loan_term,
        MIN(t2.days) OVER (PARTITION BY t1.customer_id) AS min_loan_term,
        MAX(t2.days) OVER (PARTITION BY t1.customer_id) AS max_loan_term,
        CASE
            WHEN (SELECT MAX(t4.status_dt)
                  FROM test_data_contract_status t4
                  WHERE t4.contract_id = t1.contract_id
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_closed,
        CASE 
            WHEN (SELECT MAX(t4.status_dt)
     FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) - t1.issue_dt 
        END AS usage_days,
        CASE 
            WHEN (SELECT MAX(t4.status_dt) 
                  FROM test_data_contract_status t4 
                  WHERE t4.contract_id = t1.contract_id 
                    AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')) IS NOT NULL
            THEN (SELECT MAX(t4.status_dt) FROM test_data_contract_status t4 WHERE t4.contract_id = t1.contract_id) 
                 - (SELECT MAX(payment_dt) 
                    FROM test_data_contract_conditions_payment_plan 
                    WHERE condition_id = t2.condition_id) 
        END AS dev_days,
        t1.issue_dt - LAG(
            (SELECT MAX(t4.status_dt) 
             FROM test_data_contract_status t4 
             WHERE t4.contract_id = t1.contract_id 
               AND t4.status_type IN ('Закрыт', 'Договор закрыт с переплатой', 'Переоформлен')
            )) OVER (PARTITION BY t1.customer_id ORDER BY t1.issue_dt) AS delay_days,
        CASE 
            WHEN (SELECT COUNT(*) 
                  FROM test_data_contract t1_next
                  WHERE t1_next.customer_id = t1.customer_id 
                    AND t1_next.contract_id > t1.contract_id) > 0 THEN 1 
            ELSE 0 
        END AS has_next
    FROM test_data_contract t1
    LEFT JOIN test_data_contract_conditions t2 ON t2.contract_id = t1.contract_id
    LEFT JOIN test_data_contract_conditions_payment_plan t3 ON t3.condition_id = t2.condition_id
    LEFT JOIN plc ON plc.condition_id = t2.condition_id
),
contracted_sums AS (
    SELECT 
        customer_id, 
        contract_serial_number,
        SUM(loan_amount) AS total_amount  -- Общая сумма контракта
    FROM base_data
    GROUP BY customer_id, contract_serial_number
),
customer_groups AS (
    SELECT 
        customer_id,
        EXTRACT(YEAR FROM first_issue_dt) AS start_year,
        EXTRACT(MONTH FROM first_issue_dt) AS start_month,
        MAX(contract_serial_number) AS max_contract_num  -- Макс. порядковый номер контракта
    FROM base_data
    GROUP BY customer_id, start_year, start_month
),
ranked_customers AS (
    SELECT 
        *,
        DENSE_RANK() OVER (
            PARTITION BY start_year, start_month 
            ORDER BY max_contract_num DESC
        ) AS group_rank
    FROM customer_groups
),
filtered_clients AS (
    SELECT *
    FROM ranked_customers
    WHERE group_rank = 1  -- Выбор клиентов с макс. контрактом в группе
),
contract_ratios AS (
    SELECT 
        f.start_year,
        f.start_month,
        f.customer_id,
        c1.total_amount AS first_amount,  -- Сумма первого контракта (serial=1)
        c2.total_amount AS last_amount    -- Сумма последнего контракта (serial=max)
    FROM filtered_clients f
    LEFT JOIN contracted_sums c1 
        ON f.customer_id = c1.customer_id AND c1.contract_serial_number = 1
    LEFT JOIN contracted_sums c2 
        ON f.customer_id = c2.customer_id AND c2.contract_serial_number = f.max_contract_num
)
SELECT 
    start_year,
    start_month,
    customer_id,
    last_amount / NULLIF(first_amount, 0) AS increase_ratio  -- Рассчитанный коэффициент
FROM contract_ratios
ORDER BY start_year, start_month;