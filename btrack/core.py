import sqlite3

def init_db(db_path):

	with sqlite3.connect(db_path) as conn:

		c = conn.cursor()

		c.execute('''
		CREATE TABLE btrack
		(path TEXT PRIMARY KEY,
		hash TEXT,
		file_mod_time TEXT,
		hash_mod_time TEXT)
		''')

		c.execute('''
		CREATE INDEX btrack_hash_idx
		ON btrack(hash)
		''')

def main():

	pass
