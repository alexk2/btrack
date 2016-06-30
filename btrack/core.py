import sqlite3
import os
import hashlib
import time

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

class Delta:
	def __init__(self):
		self.created = []
		self.deleted = []
		self.modified = []
		self.moved = []
		self.touched = []
		self.deteriorated = []

class FileState:
	def __init__(self, file_path, file_hash, file_mod_time):
		self.path = file_path
		self.hash = file_hash
		self.file_mod_time = file_mod_time

class FileMovement:
	def __init__(self, old_path, new_path):
		self.old_path = old_path
		self.new_path = new_path

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

def generate_delta(db_path, dir_path):
	with sqlite3.connect(db_path) as conn:
		conn.row_factory = sqlite3.Row
		c = conn.cursor()
		delta = Delta()

		prev_paths = [x[0] for x in c.execute('SELECT path FROM btrack').fetchall()]
		unmatched_paths = set(prev_paths)

		for root, dirs, files in os.walk(dir_path):
			for f in files:
				curr_path = os.path.join(root,f)
				curr_hash = compute_file_hash(curr_path)
				file_stats = os.stat(curr_path)
				curr_file_mod_time = time.strftime(DATE_FORMAT, \
					time.localtime(file_stats.st_mtime))
				curr_state = FileState(curr_path, curr_hash, curr_file_mod_time)

				if curr_path in unmatched_paths:
					unmatched_paths.remove(curr_path)

					path_record = c.execute('''
					SELECT * FROM btrack
					WHERE path = ?
					''', [curr_path]).fetchone()

					if curr_hash == path_record['hash']:
						if curr_file_mod_time != path_record['file_mod_time']:
							delta.touched.append(curr_state)
					else:
						if curr_file_mod_time != path_record['file_mod_time']:
							delta.modified.append(curr_state)
						else:
							delta.deteriorated.append(curr_state)
				else:
					hash_records = c.execute('''
					SELECT * FROM btrack
					WHERE hash = ?
					''', [curr_hash]).fetchall()

					match_found = False
					for hash_record in hash_records:
						if hash_record['path'] in unmatched_paths:
							match_found = True
							unmatched_paths.remove(hash_record['path'])

							delta.moved.append(FileMovement(hash_record['path'], \
								curr_path))
							if curr_file_mod_time != hash_record['file_mod_time']:
								delta.touched.append(curr_state)

							break

					if not match_found:
						delta.created.append(curr_state)

	delta.deleted = list(unmatched_paths)
	return delta

def apply_delta(delta, db_path):
	with sqlite3.connect(db_path) as conn:
		c = conn.cursor()

		for fm in delta.moved:
			c.execute('''
			UPDATE btrack
			SET path=?
			WHERE path=?
			''', [fm.new_path, fm.old_path])

		for fs in delta.created:
			hash_mod_time = time.strftime(DATE_FORMAT, time.localtime())
			c.execute('''
			INSERT INTO btrack (path, hash, file_mod_time, hash_mod_time)
			VALUES (?, ?, ?, ?)
			''', [fs.path, fs.hash, fs.file_mod_time, hash_mod_time])

		for path in delta.deleted:
			c.execute('''
			DELETE FROM btrack
			WHERE path=?
			''', [path])

		for fs in delta.modified:
			hash_mod_time = time.strftime(DATE_FORMAT, time.localtime())
			c.execute('''
			UPDATE btrack
			SET hash=?, file_mod_time=?, hash_mod_time=?
			WHERE path=?
			''', [fs.hash, fs.file_mod_time, hash_mod_time, fs.path])

		for fs in delta.touched:
			c.execute('''
			UPDATE btrack
			SET file_mod_time=?
			WHERE path=?
			''', [fs.file_mod_time, fs.path])

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
