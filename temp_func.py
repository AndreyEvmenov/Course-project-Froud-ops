import jaydebeapi
import pandas as pd
import sqlite3


conn = jaydebeapi.connect(
	'oracle.jdbc.driver.OracleDriver',
	'jdbc:oracle:thin:de1h/xxxxxxxxxxxx@de-oracle.chronosavant.ru:1521/deoracle',
	['de1h','xxxxxxxxxxxx'],
	'ojdbc7.jar')

curs = conn.cursor()

connect = sqlite3.connect('bank.db')
cursor = connect.cursor()

def drop_tbl(table):
	curs.execute(f'DROP TABLE {table}')


def drop_HIST_tables():
	curs.execute('DROP TABLE de1h.s_06_DWH_DIM_transact_HIST')
	curs.execute('DROP view de1h.s_06_v_transact_HIST')
	curs.execute('DROP TABLE de1h.s_06_DWH_DIM_pssp_blklst_HIST')
	curs.execute('DROP view de1h.s_06_v_pssp_blklst_HIST')
	curs.execute('DROP TABLE de1h.s_06_DWH_DIM_terminals_HIST')
	curs.execute('DROP view de1h.s_06_v_terminals_HIST')
	# curs.execute('DROP TABLE de1h.s_06_REP_FRAUD')


def sql_time():
	curs.execute('SELECT SYSDATE FROM dual')
	print(curs.fetchone()[0])


def show_table(table):
	print('^^^^^^^' * 10)
	curs.execute(f'SELECT * FROM {table}')
	for row in curs.fetchall():
		print(row)
	print('-------' * 10)


def sql_count():
	curs.execute('SELECT count(*) FROM de1h.s_06_DWH_FACT_transact')
	print('В таблице de1h.s_06_DWH_FACT_transact строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_DWH_FACT_pssp_blklst')
	print('В таблице de1h.s_06_DWH_FACT_pssp_blklst строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_DWH_FACT_terminals')
	print('В таблице de1h.s_06_DWH_FACT_terminals строк:', curs.fetchone()[0])

	curs.execute('SELECT count(*) FROM de1h.s_06_DWH_DIM_transact_HIST')
	print('В таблице de1h.s_06_DWH_DIM_transact_HIST строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_DWH_DIM_pssp_blklst_HIST')
	print('В таблице de1h.s_06_DWH_DIM_pssp_blklst_HIST строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_DWH_DIM_terminals_HIST')
	print('В таблице de1h.s_06_DWH_DIM_terminals_HIST строк:', curs.fetchone()[0])

	curs.execute('SELECT count(*) FROM de1h.s_06_v_new_rows_transact')
	print('В таблице de1h.s_06_v_new_rows_transact строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_v_new_rows_pssp_blklst')
	print('В таблице de1h.s_06_v_new_rows_pssp_blklst строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_v_new_rows_terminals')
	print('В таблице de1h.s_06_v_new_rows_terminals строк:', curs.fetchone()[0])
	
	curs.execute('SELECT count(*) FROM de1h.s_06_v_dltd_rows_transact')
	print('В таблице de1h.s_06_v_dltd_rows_transact строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_v_dltd_rows_pssp_blklst')
	print('В таблице de1h.s_06_v_dltd_rows_pssp_blklst строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_v_dltd_rows_terminals')
	print('В таблице de1h.s_06_v_dltd_rows_terminals строк:', curs.fetchone()[0])

	curs.execute('SELECT count(*) FROM de1h.s_06_v_chgd_rows_transact')
	print('В таблице de1h.s_06_v_chgd_rows_transact строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_v_chgd_rows_pssp_blklst')
	print('В таблице de1h.s_06_v_chgd_rows_pssp_blklst строк:', curs.fetchone()[0])
	curs.execute('SELECT count(*) FROM de1h.s_06_v_chgd_rows_terminals')
	print('В таблице de1h.s_06_v_chgd_rows_terminals строк:', curs.fetchone()[0])


def run_sql_query():
	# curs.execute("""
		# select * from ALL_TAB_COLUMNS
		# where table_name = 'DE1H.S_06_DWH_DIM_TERMINALS_HIST'
	# """)
	curs.execute("DROP TABLE de1h.s_06_REP_FRAUD")
	# for row in curs.fetchall():
		# print(row)
	curs.execute("""
		CREATE TABLE de1h.s_06_REP_FRAUD(
			event_dt timestamp,
			passport varchar(11),
			fio varchar(60),
			phone varchar(20),
			event_type varchar(80),
			report_dt timestamp default SYSDATE
		)
	""")


def copy_clients_to_sqlite():
	
	curs.execute('''
		SELECT * from bank.clients
	''')
	df = curs.fetchall()	

	cursor.execute('''CREATE TABLE if not exists clients (
		client_id varchar(10),
		last_name varchar(20),
		first_name varchar(20),
		patronymic varchar(20),
		date_of_birth timestamp,
		passport_num varchar(20),
		passport_valid_to timestamp,
		phone varchar(20),
		create_dt timestamp,
		update_dt timestamp
		)
	''')
	
	cursor.executemany('''INSERT INTO clients (
		client_id,
		last_name,
		first_name,
		patronymic,
		date_of_birth,
		passport_num,
		passport_valid_to,
		phone,
		create_dt,
		update_dt
		) VALUES(?,?,?,?,?,?,?,?,?,?)''', df)

	cursor.execute('SELECT * FROM clients')
	for row in cursor.fetchall():
		print(row)


def copy_accounts_to_sqlite():
	
	curs.execute('''
		SELECT * from bank.accounts
	''')
	df = curs.fetchall()	

	cursor.execute('''CREATE TABLE if not exists accounts (
		account varchar(30),
		valid_to timestamp,
		client varchar(10),
		create_dt timestamp,
		update_dt timestamp
		)
	''')
	
	cursor.executemany('''INSERT INTO accounts (
		account,
		valid_to,
		client,
		create_dt,
		update_dt
		) VALUES(?,?,?,?,?)''', df)

	cursor.execute('SELECT * FROM accounts')
	for row in cursor.fetchall():
		print(row)


def copy_cards_to_sqlite():
	
	curs.execute('''
		SELECT * from bank.cards
	''')
	df = curs.fetchall()	

	cursor.execute('''CREATE TABLE if not exists cards (
		card_num varchar(30),
		account varchar(30),
		create_dt timestamp,
		update_dt timestamp
		)
	''')
	
	cursor.executemany('''INSERT INTO cards (
		card_num,
		account,
		create_dt,
		update_dt
		) VALUES(?,?,?,?)''', df)

	cursor.execute('SELECT * FROM cards')
	for row in cursor.fetchall():
		print(row)


# drop_HIST_tables()

# sql_count()
# run_sql_query()
# drop_HIST_tables()

# cursor.execute('DROP TABLE clients')
# copy_clients_to_sqlite()
# copy_accounts_to_sqlite()
copy_cards_to_sqlite()


