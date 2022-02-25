import jaydebeapi
import pandas as pd


conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de1h/xxxxxxxxxxxx@de-oracle.chronosavant.ru:1521/deoracle',
	['de1h','xxxxxxxxxxxx'],
	'ojdbc7.jar')

curs = conn.cursor()


def create_STG_tables(): # 
	"""
	Cоздание таблиц для первоначальной загрузки данных из xlsx, csv
	Так как таблицы временные, предварительно все таблицы удаляем
	и создаем заново.
	"""
	try:
		curs.execute('DROP TABLE de1h.s_06_STG_transact')
		print('>>> Таблица "de1h.s_06_STG_transact" успешно сброшена')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_STG_transact" уже удалена')

	try:
		curs.execute('DROP TABLE de1h.s_06_STG_pssp_blklst')
		print('>>> Таблица "de1h.s_06_STG_pssp_blklst" успешно сброшена')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_STG_pssp_blklst" уже удалена')

	try:
		curs.execute('DROP TABLE de1h.s_06_STG_terminals')
		print('>>> Таблица "de1h.s_06_STG_terminals" успешно сброшена')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_STG_terminals" уже удалена')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_STG_transact(
				transaction_id varchar(11),
				transaction_date varchar(25),
				amount number(10, 2),
				card_num varchar(19),
				oper_type varchar(20),
				oper_result varchar(20),
				terminal varchar(15)
			)
		''')
		print('>>> Таблица "de1h.s_06_STG_transact" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_STG_transact" уже существует')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_STG_pssp_blklst(
				passport_date varchar(20),
				passport varchar(11)
			)
		''')
		print('>>> Таблица "de1h.s_06_STG_pssp_blklst" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_STG_pssp_blklst" уже существует')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_STG_terminals(
				terminal_id varchar(6),
				terminal_type varchar(4),
				terminal_city varchar(40),
				terminal_address varchar(80)
			)
		''')
		print('>>> Таблица "de1h.s_06_STG_terminals" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_STG_terminals" уже существует')


def create_FACT_tables():
	"""
	Функция cоздания таблиц фактовю. Так как таблицы предназначены для закгрузки инкремента одного дня,
	предварительно все таблицы удаляем и создаем заново.
	"""
	try:
		curs.execute('DROP TABLE de1h.s_06_DWH_FACT_transact')
		print('>>> Таблица "de1h.s_06_DWH_FACT_transact" успешно сброшена')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_FACT_transact" уже удалена')

	try:
		curs.execute('DROP TABLE de1h.s_06_DWH_FACT_pssp_blklst')
		print('>>> Таблица "de1h.s_06_DWH_FACT_pssp_blklst" успешно сброшена')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_FACT_pssp_blklst" уже удалена')

	try:
		curs.execute('DROP TABLE de1h.s_06_DWH_FACT_terminals')
		print('>>> Таблица "de1h.s_06_DWH_FACT_terminals" успешно сброшена')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_FACT_terminals" уже удалена')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_DWH_FACT_transact(
				transaction_id varchar(11),
				transaction_date timestamp,
				amount number(10, 2),
				card_num varchar(19),
				oper_type varchar(20),
				oper_result varchar(20),
				terminal varchar(15)
			)
		''')
		print('>>> Таблица "de1h.s_06_DWH_FACT_transact" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_FACT_transact" уже существует')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_DWH_FACT_pssp_blklst(
				passport_date timestamp,
				passport varchar(11)
			)
		''')
		print('>>> Таблица "de1h.s_06_DWH_FACT_pssp_blklst" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_FACT_pssp_blklst" уже существует')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_DWH_FACT_terminals(
				terminal_id varchar(6),
				terminal_type varchar(4),
				terminal_city varchar(40),
				terminal_address varchar(80)
			)
		''')
		print('>>> Таблица "de1h.s_06_DWH_FACT_terminals" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_FACT_terminals" уже существует')


def create_HIST_tables():
	"""
	Функция создания исторических таблиц транзакций, черного списка паспортов и терминалов
	и представлений с актуальными на текущий момент записями
	"""
	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_DWH_DIM_transact_HIST(
				transaction_id varchar(11),
				transaction_date timestamp,
				amount number(10, 2),
				card_num varchar(19),
				oper_type varchar(20),
				oper_result varchar(20),
				terminal varchar(15),
				deleted_flg integer default 0,
				effective_from timestamp default SYSDATE,
				effective_to timestamp default timestamp '2999-12-31 23:59:59'
			)
		''')
		print('>>> Таблица "de1h.s_06_DWH_DIM_transact_HIST" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_DIM_transact_HIST" уже существует')


	try:
		curs.execute('''
			CREATE view de1h.s_06_v_transact_HIST as
				SELECT
					transaction_id,
					transaction_date,
					amount,
					card_num,
					oper_type,
					oper_result,
					terminal
				FROM de1h.s_06_DWH_DIM_transact_HIST
				WHERE current_timestamp BETWEEN effective_from AND effective_to AND deleted_flg = 0
		''')
		print('>>> Предсталение "de1h.s_06_v_transact_HIST" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_transact_HIST" уже существует')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_DWH_DIM_pssp_blklst_HIST(
				passport_date timestamp,
				passport varchar(11),
				deleted_flg integer default 0,
				effective_from timestamp default SYSDATE,
				effective_to timestamp default timestamp '2999-12-31 23:59:59'
			)
		''')
		print('>>> Таблица "de1h.s_06_DWH_DIM_pssp_blklst_HIST" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_DIM_pssp_blklst_HIST" уже существует')

	try:
		curs.execute('''
			CREATE view de1h.s_06_v_pssp_blklst_HIST as
				SELECT
					passport_date,
					passport
				FROM de1h.s_06_DWH_DIM_pssp_blklst_HIST
				WHERE current_timestamp BETWEEN effective_from AND effective_to AND deleted_flg = 0
		''')
		print('>>> Предсталение "de1h.s_06_v_pssp_blklst_HIST" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_pssp_blklst_HIST" уже существует')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_DWH_DIM_terminals_HIST(
				terminal_id varchar(6),
				terminal_type varchar(4),
				terminal_city varchar(40),
				terminal_address varchar(80),
				deleted_flg integer default 0,
				effective_from timestamp default SYSDATE,
				effective_to timestamp default timestamp '2999-12-31 23:59:59'
			)
		''')
		print('>>> Таблица "de1h.s_06_DWH_DIM_terminals_HIST" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_DWH_DIM_terminals_HIST" уже существует')

	try:
		curs.execute('''
			CREATE view de1h.s_06_v_terminals_HIST as
				SELECT
					terminal_id,
					terminal_type,
					terminal_city,
					terminal_address
				FROM de1h.s_06_DWH_DIM_terminals_HIST
				WHERE current_timestamp BETWEEN effective_from AND effective_to AND deleted_flg = 0
		''')
		print('>>> Предсталение "de1h.s_06_v_terminals_HIST" успешно создано')
	except jaydebeapi.DatabaseError:
		print('XXX Предсталение "de1h.s_06_v_terminals_HIST" уже существует')

	try:
		curs.execute('''
			CREATE TABLE de1h.s_06_REP_FRAUD(
				event_dt timestamp,
				passport varchar(11),
				fio varchar(60),
				phone varchar(20),
				event_type varchar(80),
				report_dt timestamp default SYSDATE
			)
		''')
		print('>>> Таблица "de1h.s_06_REP_FRAUD" успешно создана')
	except jaydebeapi.DatabaseError:
		print('XXX Таблица "de1h.s_06_REP_FRAUD" уже существует')


