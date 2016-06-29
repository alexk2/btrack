import sqlite3
import hashlib

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

def compute_file_hash(file_path):

	BUFFER_SIZE = 1 << 24
	sha256 = hashlib.sha256()

	with open(file_path, 'r') as f:

		while True:
			data = f.read(BUFFER_SIZE)
			if not data:
				break
			sha256.update(data)

	return sha256.hexdigest()

def main():

	pass
