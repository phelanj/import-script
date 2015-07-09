#!/usr/bin/python
#
# Imports data from CONTENTdm XML export files into a SQLite database.
#

import elementtree.ElementTree as ET
import os
import sqlite3
import string
import subprocess
import sys

if len(sys.argv) != 3:
	print 'You must supply the path to the data folder and name of xml file to import.'
	sys.exit(1)

DATA_PATH = os.path.abspath(str(sys.argv[1]))
if not os.path.exists(DATA_PATH):
	print DATA_PATH + ' does not exist.'
	sys.exit(1)

XML_PATH = DATA_PATH + os.path.abspath(str(sys.argv[1]))

COLLECTIONS_PATH = DATA_PATH + '/collections'

if not os.path.exists(COLLECTIONS_PATH):
	print COLLECTIONS_PATH + ' does not exist.'
	sys.exit(1)

DATABASE_PATH = DATA_PATH + '/data.sqlite'

if os.path.exists(DATABASE_PATH):
	print 'Deleting old database...'
	os.unlink(DATABASE_PATH)

ELEMENTS_TO_IMPORT = ['alternative', 'date', 'contributor', 'creator',
					'description', 'format', 'identifier', 'isPartOf',
					'isReferencedBy', 'language', 'publisher', 'references',
					'relation', 'rights', 'source', 'spatial', 'subject',
					'temporal', 'title', 'type']

SUPPORTED_IMAGE_FORMATS = ['jpg', 'jpeg', 'jp2', 'png', 'tif', 'tiff']

SUPPORTED_FORMATS = list(SUPPORTED_IMAGE_FORMATS)
SUPPORTED_FORMATS.extend(['cpd', 'mp4', 'mpg'])

sqlcon = sqlite3.connect(DATABASE_PATH)

with sqlcon:
	cur = sqlcon.cursor()
	cur.execute('CREATE TABLE digital_object(id INTEGER PRIMARY KEY, '
				+ 'alias TEXT NOT NULL, '
				+ 'pointer INTEGER NOT NULL, '
				+ 'filename TEXT NOT NULL, '
				+ 'pixel_width INTEGER, '
				+ 'pixel_height INTEGER)')
	cur.execute('CREATE TABLE dc_element(id INTEGER PRIMARY KEY, '
				+ 'digital_object_id INTEGER, '
				+ 'name TEXT NOT NULL, '
				+ 'label TEXT, '
				+ 'value TEXT NOT NULL)')
	cur.execute('CREATE TABLE compound_object_page(id INTEGER PRIMARY KEY, '
				+ 'digital_object_id INTEGER NOT NULL, '
				+ 'page INTEGER NOT NULL, '
				+ 'pointer INTEGER NOT NULL, '
				+ 'title TEXT, '
				+ 'filename TEXT NOT NULL)')
	cur.execute('CREATE INDEX digital_object_idx ON digital_object (id)')
	cur.execute('CREATE INDEX digital_object_alias_idx ON digital_object (alias)')
	cur.execute('CREATE INDEX digital_object_pointer_idx ON digital_object (pointer)')
	cur.execute('CREATE INDEX dc_element_idx ON dc_element (id)')
	cur.execute('CREATE INDEX dc_element_name_idx ON dc_element (name)')
	cur.execute('CREATE INDEX dc_element_value_idx ON dc_element (value)')
	cur.execute('CREATE INDEX compound_object_page_idx ON compound_object_page (id)')


def importXMLFile(path):
	print 'Importing ' + path
	tree = ET.parse(path)
	root = tree.getroot()

	base = os.path.basename(path)
	os.path.splitext(base)
	alias = '/' + os.path.splitext(base)[0]

	importCounter = 1
	allRecords = root.findall('record')

	for record in allRecords:
		filename = record.findtext('cdmfile')
		parts = string.split(filename, '.')
		extension = parts[-1].lower()

		print 'Importing ' + alias + ' ' + filename + ' (' + str(importCounter) + '/' + str(len(allRecords)) + ')'

		# import only supported file types
		parts = string.split(filename, '.')
		with sqlcon:
			cur = sqlcon.cursor()
			if extension in SUPPORTED_FORMATS:
				# if the object is an image, get its dimensions and put them in
				# the database
				if extension in SUPPORTED_IMAGE_FORMATS:
					fullPath = COLLECTIONS_PATH + alias + '/image/' + filename
	
					output = subprocess.check_output(
						['identify', '-format', '%[fx:w] %[fx:h]', fullPath])
					parts = output.split(' ')
					if len(parts) == 2:
						insertObject(cur, alias, record.findtext('cdmid'),
									 filename, parts[0], parts[1])
					else:
						insertObject(cur, alias, record.findtext('cdmid'),
									 filename)
				else:
					insertObject(cur, alias, record.findtext('cdmid'), filename)
	
				digital_object_id = cur.lastrowid
	
				# metadata elements
				for element in ELEMENTS_TO_IMPORT:
					value = record.findtext(element)
					if value:
						if element == 'subject':
							for subject in splitSubjects(value):
								insertElement(cur, digital_object_id, element,
											  None, subject)
						else:
							insertElement(cur, digital_object_id, element,
										  None, value)
				# compound object pages need additional handling
				if extension == 'cpd':
					# We have to read the corresponding .cpd file on disk to get
					# the filename and pointer.
					cpdPath = COLLECTIONS_PATH + alias + '/image/' + filename
					cpdTree = ET.parse(cpdPath)
					cpdRoot = cpdTree.getroot()

					pageCounter = 1;
					for cpdPage in cpdRoot.findall('page'):
						insertCompoundObjectPage(cur, digital_object_id,
												 pageCounter,
												 cpdPage.findtext('pageptr'),
												 cpdPage.findtext('pagetitle'),
												 cpdPage.findtext('pagefile'))
						pageCounter += 1
		importCounter += 1

def insertObject(cursor, alias, pointer, filename, pixel_width=None,
				 pixel_height=None):
	cursor.execute('INSERT INTO digital_object '
				   + 'VALUES(:id, :alias, :pointer, :filename, :pixel_width, '
				   + ':pixel_height)', {
		'id': None,
		'alias': alias,
		'pointer': pointer,
		'filename': filename,
		'pixel_width': pixel_width,
		'pixel_height': pixel_height
	})

def insertCompoundObjectPage(cursor, digital_object_id, page, pointer, title,
							 filename):
	cursor.execute('INSERT INTO compound_object_page VALUES(:id, '
				   + ':digital_object_id, :page, :pointer, :title, :filename)', {
		'id': None,
		'digital_object_id': digital_object_id,
		'page': page,
		'pointer': pointer,
		'title': title,
		'filename': filename
	})

def insertElement(cursor, digital_object_id, name, label, value):
	cursor.execute('INSERT INTO dc_element VALUES(:id, :digital_object_id, '
				   + ':name, :label, :value)', {
		'id': None,
		'digital_object_id': digital_object_id,
		'name': name,
		'label': label,
		'value': value
	})

# Splits a semicolon-separated subject string into an array of subjects
def splitSubjects(subjectString):
	subjects = string.split(subjectString, ';')
	returnedSubjects = []

	for subject in subjects:
		subject = subject.strip()
		if subject:
			returnedSubjects.append(subject)
	return returnedSubjects


for (dirpath, dirnames, filenames) in os.walk(COLLECTIONS_PATH):
	for file in filenames:
		if dirpath == COLLECTIONS_PATH and file.endswith('.xml'):
			fullPath = os.path.join(dirpath, file)
			importXMLFile(fullPath)
