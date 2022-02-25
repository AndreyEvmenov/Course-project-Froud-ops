import jaydebeapi
import pandas as pd


conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de1h/xxxxxxxxxxxx@de-oracle.chronosavant.ru:1521/deoracle',
	['de1h','xxxxxxxxxxxx'],
	'ojdbc7.jar')

curs = conn.cursor()


def csv_xlsx_to_STG_tables(filename_trnsct, filename_blklst, filename_terminal):
	"""
	Функция загрузки данных из xlsx и csv файлов во временные таблицы.
	"""
	df = pd.read_csv(filename_trnsct, sep=';')
	curs.executemany('''insert into de1h.s_06_STG_transact (
		transaction_id,
		transaction_date,
		amount,
		card_num,
		oper_type,
		oper_result,
		terminal
		) VALUES(?,?,?,?,?,?,?)''', df.values.tolist())

	df = pd.read_excel(filename_blklst, dtype={'date':str})
	curs.executemany('''insert into de1h.s_06_STG_pssp_blklst (
		passport_date,
		passport
		) VALUES(?,?)''', df.values.tolist())	


	df = pd.read_excel(filename_terminal)
	curs.executemany('''insert into de1h.s_06_STG_terminals (
		terminal_id,
		terminal_type,
		terminal_city,
		terminal_address
		) VALUES(?,?,?,?)''', df.values.tolist())


def load_to_FACT_tables():
	"""
	Функция звгрузки данных из первоначальных (STG) таблиц в таблицы фактов
	с приведением полей дат к корректному формату timestamp
	"""
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_FACT_transact (
			transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal)
		SELECT transaction_id, TO_TIMESTAMP(transaction_date, 'YYYY-MM-DD HH24:MI:SS'), amount, card_num, oper_type, oper_result, terminal
		FROM de1h.s_06_STG_transact
	''')

	curs.execute('''
		INSERT INTO de1h.s_06_DWH_FACT_pssp_blklst (passport_date, passport)
		SELECT TO_TIMESTAMP(passport_date, 'YYYY-MM-DD HH24:MI:SS'), passport
		FROM de1h.s_06_STG_pssp_blklst
	''')

	curs.execute('''
		INSERT INTO de1h.s_06_DWH_FACT_terminals (terminal_id, terminal_type, terminal_city, terminal_address)
		SELECT terminal_id, terminal_type, terminal_city, terminal_address
		FROM de1h.s_06_STG_terminals
	''')


def new_FACTs():
	"""
	Функция создания представлений, содержащих новые факты,
	которые оотсутствуют в историчемих таблицах
	"""
	try:
		curs.execute('DROP VIEW de1h.s_06_v_new_rows_transact')
		print('>>> Предсталение "de1h.s_06_v_new_rows_transact" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_new_rows_transact" уже удалено')

	try:
		curs.execute('DROP VIEW de1h.s_06_v_new_rows_pssp_blklst')
		print('>>> Предсталение "de1h.s_06_v_new_rows_pssp_blklst" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_new_rows_pssp_blklst" уже удалено')

	try:
		curs.execute('DROP VIEW de1h.s_06_v_new_rows_terminals')
		print('>>> Предсталение "de1h.s_06_v_new_rows_terminals" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_new_rows_terminals" уже удалено')

	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_new_rows_transact as
				SELECT
					t1.transaction_id,
					t1.transaction_date,
					t1.amount,
					t1.card_num,
					t1.oper_type,
					t1.oper_result,
					t1.terminal
				FROM de1h.s_06_DWH_FACT_transact t1
				LEFT JOIN de1h.s_06_v_transact_HIST t2
				on t1.transaction_id = t2.transaction_id 
				where t2.transaction_id is null
			''')
		print('>>> Предсталение "de1h.s_06_v_new_rows_transact" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_new_rows_transact" уже существует')
	
	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_new_rows_pssp_blklst as
				SELECT
					t1.passport_date,
					t1.passport
				FROM de1h.s_06_DWH_FACT_pssp_blklst t1
				LEFT JOIN de1h.s_06_v_pssp_blklst_HIST t2
				on t1.passport = t2.passport
				where t2.passport is null
			''')
		print('>>> Предсталение "de1h.s_06_v_new_rows_pssp_blklst" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_new_rows_pssp_blklst" уже существует')
	
	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_new_rows_terminals as
				SELECT
					t1.terminal_id,
					t1.terminal_type,
					t1.terminal_city,
					t1.terminal_address
				FROM de1h.s_06_DWH_FACT_terminals t1
				LEFT JOIN de1h.s_06_v_terminals_HIST t2
				on t1.terminal_id = t2.terminal_id
				where t2.terminal_id is null
			''')
		print('>>> Предсталение "de1h.s_06_v_new_rows_terminals" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_new_rows_terminals" уже существует')


def deleted_FACTs():
	"""
	Функция создания представлений, содержащих удаленные факты,
	которые есть в историчемих таблицах, но нет в фактических
	"""
	try:
		curs.execute('DROP VIEW de1h.s_06_v_dltd_rows_transact')
		print('>>> Предсталение "de1h.s_06_v_dltd_rows_transact" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_dltd_rows_transact" уже удалено')

	try:
		curs.execute('DROP VIEW de1h.s_06_v_dltd_rows_pssp_blklst')
		print('>>> Предсталение "de1h.s_06_v_dltd_rows_pssp_blklst" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_dltd_rows_pssp_blklst" уже удалено')

	try:
		curs.execute('DROP VIEW de1h.s_06_v_dltd_rows_terminals')
		print('>>> Предсталение "de1h.s_06_v_dltd_rows_terminals" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_dltd_rows_terminals" уже удалено')

	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_dltd_rows_transact as
				SELECT
					t1.transaction_id,
					t1.transaction_date,
					t1.amount,
					t1.card_num,
					t1.oper_type,
					t1.oper_result,
					t1.terminal
				FROM de1h.s_06_v_transact_HIST t1
				LEFT JOIN  de1h.s_06_DWH_FACT_transact t2 
				on t1.transaction_id = t2.transaction_id 
				where t2.transaction_id is null
			''')
		print('>>> Предсталение "de1h.s_06_v_dltd_rows_transact" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_dltd_rows_transact" уже существует')
	
	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_dltd_rows_pssp_blklst as
				SELECT
					t1.passport_date,
					t1.passport
				FROM de1h.s_06_v_pssp_blklst_HIST t1
				LEFT JOIN de1h.s_06_DWH_FACT_pssp_blklst t2
				on t1.passport = t2.passport
				where t2.passport is null
			''')
		print('>>> Предсталение "de1h.s_06_v_dltd_rows_pssp_blklst" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_dltd_rows_pssp_blklst" уже существует')
	
	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_dltd_rows_terminals as
				SELECT
					t1.terminal_id,
					t1.terminal_type,
					t1.terminal_city,
					t1.terminal_address
				FROM de1h.s_06_v_terminals_HIST t1
				LEFT JOIN de1h.s_06_DWH_FACT_terminals t2
				on t1.terminal_id = t2.terminal_id
				where t2.terminal_id is null
			''')
		print('>>> Предсталение "de1h.s_06_v_dltd_rows_terminals" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_dltd_rows_terminals" уже существует')


def changed_FACTs():
	"""
	Функция создания представлений, содержащих измененные факты,
	которые изменились в фактических таблицах относительно исторических
	"""
	try:
		curs.execute('DROP VIEW de1h.s_06_v_chgd_rows_transact')
		print('>>> Предсталение "de1h.s_06_v_chgd_rows_transact" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_chgd_rows_transact" уже удалено')

	try:
		curs.execute('DROP VIEW de1h.s_06_v_chgd_rows_pssp_blklst')
		print('>>> Предсталение "de1h.s_06_v_chgd_rows_pssp_blklst" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_chgd_rows_pssp_blklst" уже удалено')

	try:
		curs.execute('DROP VIEW de1h.s_06_v_chgd_rows_terminals')
		print('>>> Предсталение "de1h.s_06_v_chgd_rows_terminals" успешно сброшено')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_chgd_rows_terminals" уже удалено')

	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_chgd_rows_transact as
				SELECT
					t1.transaction_id,
					t1.transaction_date,
					t1.amount,
					t1.card_num,
					t1.oper_type,
					t1.oper_result,
					t1.terminal
				FROM de1h.s_06_DWH_FACT_transact t1
				INNER JOIN de1h.s_06_v_transact_HIST t2
				on t1.transaction_id = t2.transaction_id and
					(t1.transaction_date <> t2.transaction_date or
					t1.amount <> t2.amount or
					t1.card_num <> t2.card_num or
					t1.oper_type <> t2.oper_type or
					t1.oper_result <> t2.oper_result or
					t1.terminal <> t2.terminal)
			''')

		print('>>> Предсталение "de1h.s_06_v_chgd_rows_transact" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_chgd_rows_transact" уже существует')
	
	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_chgd_rows_pssp_blklst as
				SELECT
					t1.passport_date,
					t1.passport
				FROM de1h.s_06_DWH_FACT_pssp_blklst t1
				INNER JOIN de1h.s_06_v_pssp_blklst_HIST t2
				on t1.passport = t2.passport and
					(t1.passport_date <> t2.passport_date)
			''')

		print('>>> Предсталение "de1h.s_06_v_chgd_rows_pssp_blklst" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_chgd_rows_pssp_blklst" уже существует')
	
	try:
		curs.execute('''
			CREATE VIEW de1h.s_06_v_chgd_rows_terminals as
				SELECT
					t1.terminal_id,
					t1.terminal_type,
					t1.terminal_city,
					t1.terminal_address
				FROM de1h.s_06_DWH_FACT_terminals t1
				INNER JOIN de1h.s_06_v_terminals_HIST t2
				on t1.terminal_id = t2.terminal_id and
					(t1.terminal_type <> t2.terminal_type or
					t1.terminal_city <> t2.terminal_city or
					t1.terminal_address <> t2.terminal_address)
			''')

		print('>>> Предсталение "de1h.s_06_v_chgd_rows_terminals" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_chgd_rows_terminals" уже существует')


def change_HIST_tables():
	# обновляем дату effective_to в исторической таблице транзакций, устанавливаем тек. время -1 сек
	# для строк, которые найдены в представлении измененных строк
	curs.execute('''
		UPDATE de1h.s_06_DWH_DIM_transact_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where transaction_id in (select transaction_id from de1h.s_06_v_chgd_rows_transact)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
	# обновляем дату effective_to в исторической таблице транзакций, устанавливаем тек. время -1 сек
	# для строк, которые найдены в представлении удаленных строк
	curs.execute('''
		UPDATE de1h.s_06_DWH_DIM_transact_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where transaction_id in (select transaction_id from de1h.s_06_v_dltd_rows_transact)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
	# Добавляем в историческую таблицу строки из представления новых строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_transact_HIST (transaction_id, transaction_date,
												amount, card_num, oper_type, oper_result, terminal)
		SELECT transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal
		FROM de1h.s_06_v_new_rows_transact
	''')
	# Добавляем в историческую таблицу строки из представления измененных строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_transact_HIST (transaction_id, transaction_date,
												amount, card_num, oper_type, oper_result, terminal)
		SELECT transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal
		FROM de1h.s_06_v_chgd_rows_transact
	''')
	# Добавляем в историческую таблицу строки из представления удаленных строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_transact_HIST (transaction_id, transaction_date,
												amount, card_num, oper_type, oper_result, terminal, deleted_flg)
		SELECT transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal, 1
		FROM de1h.s_06_v_dltd_rows_transact
	''')
	# ------------------------------------------------------------------------------------------------------------
	# обновляем дату effective_to в исторической таблице паспортов, устанавливаем тек. время -1 сек
	# для строк, которые найдены в представлении измененных строк
	curs.execute('''
		UPDATE de1h.s_06_DWH_DIM_pssp_blklst_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where passport in (select passport from de1h.s_06_v_chgd_rows_pssp_blklst)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
	# обновляем дату effective_to в исторической таблице паспортов, устанавливаем тек. время -1 сек
	# для строк, которые найдены в представлении удаленных строк
	curs.execute('''
		UPDATE de1h.s_06_DWH_DIM_pssp_blklst_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where passport in (select passport from de1h.s_06_v_dltd_rows_pssp_blklst)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
	# Добавляем в историческую таблицу строки из представления новых строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_pssp_blklst_HIST (passport_date, passport)
		SELECT passport_date, passport
		FROM de1h.s_06_v_new_rows_pssp_blklst
	''')
	# Добавляем в историческую таблицу строки из представления измененных строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_pssp_blklst_HIST (passport_date, passport)
		SELECT passport_date, passport
		FROM de1h.s_06_v_chgd_rows_pssp_blklst
	''')
	# Добавляем в историческую таблицу строки из представления удаленных строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_pssp_blklst_HIST (passport_date, passport, deleted_flg)
		SELECT passport_date, passport, 1
		FROM de1h.s_06_v_dltd_rows_pssp_blklst
	''')
	# ------------------------------------------------------------------------------------------------------------
	# обновляем дату effective_to в исторической таблице терминалов, устанавливаем тек. время -1 сек
	# для строк, которые найдены в представлении измененных строк
	curs.execute('''
		UPDATE de1h.s_06_DWH_DIM_terminals_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where terminal_id in (select terminal_id from de1h.s_06_v_chgd_rows_terminals)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
	# обновляем дату effective_to в исторической таблице терминалов, устанавливаем тек. время -1 сек
	# для строк, которые найдены в представлении удаленных строк
	curs.execute('''
		UPDATE de1h.s_06_DWH_DIM_terminals_HIST
		SET effective_to = SYSTIMESTAMP - NUMTODSINTERVAL(1, 'SECOND')
		where terminal_id in (select terminal_id from de1h.s_06_v_dltd_rows_terminals)
		and effective_to = timestamp '2999-12-31 23:59:59'
	''')
	# Добавляем в историческую таблицу строки из представления новых строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_terminals_HIST (terminal_id, terminal_type, terminal_city, terminal_address)
		SELECT terminal_id, terminal_type, terminal_city, terminal_address
		FROM de1h.s_06_v_new_rows_terminals
	''')
	# Добавляем в историческую таблицу строки из представления измененных строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_terminals_HIST (terminal_id, terminal_type, terminal_city, terminal_address)
		SELECT terminal_id, terminal_type, terminal_city, terminal_address
		FROM de1h.s_06_v_chgd_rows_terminals
	''')
	# Добавляем в историческую таблицу строки из представления удаленных строк
	curs.execute('''
		INSERT INTO de1h.s_06_DWH_DIM_terminals_HIST (terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg)
		SELECT terminal_id, terminal_type, terminal_city, terminal_address, 1
		FROM de1h.s_06_v_dltd_rows_terminals
	''')

