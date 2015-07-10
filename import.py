#!/usr/bin/python
#
# Imports data from the exhibits XML file into the application's SQLite
# database.
#

import elementtree.ElementTree as ET
import os
import sqlite3
import string
import subprocess
import sys

if len(sys.argv) != 2:
	print('You must supply the path to the data folder and xml file.')
	sys.exit(1)

DATA_PATH = os.path.abspath(str(sys.argv[1]))
if not os.path.exists(DATA_PATH):
	print DATA_PATH + ' does not exist.'
	sys.exit(1)
	
DATABASE_PATH = DATA_PATH + '/data.sqlite'
if not os.path.exists(DATABASE_PATH):
	print("Database does not exist. You need to run import.py first.")
	sys.exit(1);

EXHIBITS_XML_PATH = DATA_PATH +  '/' + (str(sys.argv[2]))	
if not os.path.exists(EXHIBITS_XML_PATH):
	print("""Exhibits XML file does not exist. It should be located in the
		  root of the data folder.""")
	sys.exit(1);

def idOfObject(cursor, alias, pointer):
	sql = """SELECT id
	FROM digital_object
	WHERE alias = :alias AND pointer = :pointer"""
	cursor.execute(sql, {
		'alias': alias,
		'pointer': pointer
	})
	row = cursor.fetchone()
	return int(row[0]) if row is not None else False

sqlcon = sqlite3.connect(DATABASE_PATH)

with sqlcon:
	cur = sqlcon.cursor()
	
	print('Deleting current exhibits data...')
	
	cur.execute('DROP TABLE IF EXISTS exhibit')
	cur.execute('DROP TABLE IF EXISTS digital_object_exhibit')

	print('Creating exhibits tables...')

	cur.execute("""CREATE TABLE IF NOT EXISTS exhibit(
				id INTEGER PRIMARY KEY,
				name TEXT)""")
	cur.execute("""CREATE TABLE IF NOT EXISTS digital_object_exhibit(
				id INTEGER PRIMARY KEY,
				digital_object_id INTEGER NOT NULL,
				exhibit_id INTEGER NOT NULL,
				position INTEGER NOT NULL,
				caption VARCHAR)""")
	
	tree = ET.parse(EXHIBITS_XML_PATH)
	root = tree.getroot()
	
	numImportedExhibits = 0
	numImportedObjects = 0
	numSkippedObjects = 0
	allExhibits = root.findall('exhibit')
	
	for exhibit in allExhibits:
		name = exhibit.findtext('name')
	
		print('Importing exhibit "%s"' % name)
	
		cur.execute('INSERT INTO exhibit VALUES(:id, :name)', {
			'id': None,
			'name': name
		})
	
		exhibit_id = cur.lastrowid
	
		numImportedExhibits += 1
		position = 0
	
		for object in exhibit.findall('object'):
			alias = object.findtext('alias')
			pointer = int(object.findtext('pointer'))
			caption = object.findtext('caption')
			digital_object_id = idOfObject(cur, alias, pointer)
	
			if digital_object_id:
				print('%s %d' % (alias, pointer))
	
				cur.execute("""INSERT INTO digital_object_exhibit
							VALUES(:id, :digital_object_id, :exhibit_id,
							:position, :caption)""", {
					'id': None,
					'digital_object_id': digital_object_id,
					'exhibit_id': exhibit_id,
					'position': position,
					'caption': caption
				})
				
				numImportedObjects += 1
				position += 1
			else:
				print('%s %d does not exist in the database; skipping' %
					  (alias, pointer))
				numSkippedObjects += 1

cur.close()
print('%d exhibits; %d objects imported; %d objects missing/skipped'
	  % (numImportedExhibits, numImportedObjects, numSkippedObjects))
