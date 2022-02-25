import jaydebeapi
import pandas as pd
import os
import init_tables as init
import processing as procs


conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de1h/xxxxxxxxxxxx@de-oracle.chronosavant.ru:1521/deoracle',
	['de1h','xxxxxxxxxxxx'],
	'ojdbc7.jar')

curs = conn.cursor()


def search_fraud_operations():
	# Создаем таблицу для сохранения найденных машеннических операций
	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_STG_REP_FRAUD(
				event_dt timestamp,
				passport varchar(11),
				fio varchar(60),
				phone varchar(20),
				event_type varchar(80),
				report_dt timestamp default SYSDATE
			)
		''')
		print('>>> Таблица "de1h.s_06_STG_REP_FRAUD" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_STG_REP_FRAUD" уже существует')	

	# Ищем операции совершенные с заблокированными и просроченными паспортами
	curs.execute('''
		INSERT INTO de1h.s_06_STG_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			s1.fr_date,
			s1.fr_passp,
			bank.clients.last_name||' '||bank.clients.first_name||' '||bank.clients.patronymic,
			bank.clients.phone,
			'ПРОСРОЧЕН ИЛИ ЗАБЛОКИРОВАН ПАСПОРТ',
			systimestamp
		FROM bank.clients
		inner join (
			SELECT 
				min(hi.transaction_date) fr_date,
				bcl.passport_num fr_passp
			FROM de1h.s_06_DWH_DIM_transact_HIST hi
			inner join bank.cards bc
			on hi.card_num = TRIM(TRAILING ' ' FROM bc.card_num)
			inner join bank.accounts ba
			on  bc.account = ba.account
			inner join bank.clients bcl
			on ba.client = bcl.client_id and bcl.passport_num in (
				SELECT passport_num
				FROM bank.clients
				WHERE passport_valid_to < systimestamp
				UNION
				SELECT passport
				FROM de1h.s_06_DWH_DIM_pssp_blklst_HIST
				)
			group by bcl.passport_num
			) s1
		on bank.clients.passport_num = s1.fr_passp
	''')

	# Ищем операции совершенные с недействующим договором
	curs.execute('''
		INSERT INTO de1h.s_06_STG_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			s1.fr_date,
			s1.fr_passp,
			bank.clients.last_name||' '||bank.clients.first_name||' '||bank.clients.patronymic,
			bank.clients.phone,
			'НЕДЕЙСТВУЮЩИЙ ДОГОВОР',
			systimestamp
		FROM bank.clients
		inner join (			
			SELECT
				min(hi.transaction_date) fr_date,
				bcl.passport_num fr_passp
			FROM de1h.s_06_DWH_DIM_transact_HIST hi
			inner join bank.cards bc
			on hi.card_num = TRIM(TRAILING ' ' FROM bc.card_num)
			inner join bank.accounts ba
			on ba.account = bc.account and ba.valid_to < sysdate
			inner join bank.clients bcl
			on ba.client = bcl.client_id
			group by bcl.passport_num
			) s1
		on bank.clients.passport_num = s1.fr_passp
	''')
	
	# Ищем операции cовершенные в разных городах в течение одного часа
	curs.execute('''
		INSERT INTO de1h.s_06_STG_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			s3.fr_date,
			bcl.passport_num,
			bcl.last_name||' '||bcl.first_name||' '||bcl.patronymic,
			bcl.phone,
			'ОПЕРАЦИИ В РАЗНЫХ ГОРОДАХ В ТЕЧЕНИЕ ЧАСА',
			systimestamp
		FROM bank.clients bcl
		inner join bank.accounts ba
		on ba.client = bcl.client_id
		inner join bank.cards bc
		on ba.account = bc.account
		inner join (
			SELECT
				distinct s2.cnum fr_card,
				FIRST_VALUE(s2.tdate) OVER(PARTITION BY s2.cnum ORDER BY s2.tdate) AS fr_date
			FROM
				(SELECT
					trh.card_num cnum,
					trh.transaction_date tdate, 
					teh.terminal_city city,
					LAG(trh.transaction_date) OVER(PARTITION BY trh.card_num ORDER BY trh.transaction_date) AS prv_dttm,
					LAG(teh.terminal_city) OVER(PARTITION BY trh.card_num ORDER BY trh.transaction_date) AS prv_city
				FROM de1h.s_06_DWH_DIM_transact_HIST trh
				inner join (
					SELECT
						hi.card_num cnum,
						count(distinct ter.terminal_city)
					FROM de1h.s_06_DWH_DIM_terminals_HIST ter
					inner join de1h.s_06_DWH_DIM_transact_HIST hi
					on ter.terminal_id = hi.terminal
					group by hi.card_num
					having count(distinct ter.terminal_city) > 1
					) s1
				on trh.card_num = s1.cnum
				inner join de1h.s_06_DWH_DIM_terminals_HIST teh
				on trh.terminal = teh.terminal_id
				order by trh.card_num, trh.transaction_date, teh.terminal_city
				) s2
			WHERE s2.city <> s2.prv_city and  (s2.prv_dttm + NUMTODSINTERVAL(1,'HOUR')) > s2.tdate
			) s3
		on s3.fr_card = TRIM(TRAILING ' ' FROM bc.card_num)
	''')
		
	# Ищем операции cовершенные в течение 20 минут с подбором суммы		
	curs.execute('''
		INSERT INTO de1h.s_06_STG_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)	
		SELECT
			s3.fr_date,
			bcl.passport_num,
			bcl.last_name||' '||bcl.first_name||' '||bcl.patronymic,
			bcl.phone,
			'ПОДБОР СУММЫ',
			systimestamp
		FROM bank.clients bcl
		inner join bank.accounts ba
		on ba.client = bcl.client_id
		inner join bank.cards bc
		on ba.account = bc.account
		inner join
			(select
			    s2.cn fr_card,
			    s2.td fr_date
			FROM
				(select
				    s1.card_num cn,
				    s1.transaction_date td,
				    s1.amount am1,
				    s1.transaction_id,
				    s1.OPER_RESULT or1,
				    LAG(s1.amount,1) OVER(partition by card_num order by s1.transaction_id) am2,
				    LAG(s1.amount,2) OVER(partition by card_num order by s1.transaction_id) am3,
				    LAG(s1.amount,3) OVER(partition by card_num order by s1.transaction_id) am4,
				    LAG(s1.OPER_RESULT,1) OVER(partition by card_num order by s1.transaction_id) or2,
				    LAG(s1.OPER_RESULT,2) OVER(partition by card_num order by s1.transaction_id) or3,
				    LAG(s1.OPER_RESULT,3) OVER(partition by card_num order by s1.transaction_id) or4,
				    LAG(s1.transaction_date,3) OVER(partition by card_num order by s1.transaction_id) td4
				FROM
					(select card_num,transaction_date, amount, transaction_id, OPER_RESULT
					FROM s_06_DWH_DIM_TRANSACT_HIST
					order by card_num, transaction_date
					) s1
				) s2
			where s2.am1 < s2.am2 and s2.am2 < s2.am3 and s2.am3 < s2.am4 and s2.or1 = 'SUCCESS'
			and s2.or2 = 'REJECT' and s2.or3 = 'REJECT' and s2.or4 = 'REJECT'
			and (s2.td4 + NUMTODSINTERVAL(20,'MINUTE')) > s2.td
			) s3
		on s3.fr_card = TRIM(TRAILING ' ' FROM bc.card_num)
	''')	
			
	# Добавляем строки отчета, полученные за день, в таблицу отчетов
	curs.execute('''
		INSERT INTO de1h.s_06_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
		SELECT
			t1.event_dt,
			t1.passport,
			t1.fio,
			t1.phone,
			t1.event_type,
			t1.report_dt
		FROM de1h.s_06_STG_REP_FRAUD t1
		left join de1h.s_06_REP_FRAUD t2
		on t1.event_dt = t2.event_dt and t1.passport = t2.passport and t1.event_type = t2.event_type
		WHERE t2.event_dt is NULL
	''')

	# Сбрасываем временную таблицу обнаруженных операций
	curs.execute('DROP TABLE de1h.s_06_STG_REP_FRAUD')


def one_day_ETL_process(filename_trnsct, filename_blklst, filename_terminal):
	init.create_STG_tables()
	procs.csv_xlsx_to_STG_tables(filename_trnsct, filename_blklst, filename_terminal)
	init.create_FACT_tables()
	procs.load_to_FACT_tables()
	init.create_HIST_tables()
	procs.new_FACTs()
	procs.deleted_FACTs()
	procs.changed_FACTs()
	procs.change_HIST_tables()
	search_fraud_operations()


path = 'data/'
path_new = 'data/archive/'
passp_blklst_files = []
terminals_files = []
transacts_files = []
file_list = sorted(os.listdir(path))
for item in file_list:
	if item[0:8] == 'passport':
		passp_blklst_files.append(item)
	if item[0:9] == 'terminals':
		terminals_files.append(item)
	if item[0:12] == 'transactions':
		transacts_files.append(item)
for i in range(len(transacts_files)):
	one_day_ETL_process(os.path.join(path, transacts_files[i]), os.path.join(path, passp_blklst_files[i]), 
		os.path.join(path, terminals_files[i]))
	os.renames(os.path.join(path, passp_blklst_files[i]), os.path.join(path_new, passp_blklst_files[i])+'.backup')
	os.renames(os.path.join(path, terminals_files[i]), os.path.join(path_new, terminals_files[i])+'.backup')
	os.renames(os.path.join(path, transacts_files[i]), os.path.join(path_new, transacts_files[i])+'.backup')

