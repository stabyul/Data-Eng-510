# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv
import io
import csv


DBname = "postgres"
DBuser = "postgres"
DBpwd = "BadPassword"
TableName = 'censusdata'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created

def row2vals(row):
	for key in row:
		if not row[key]:
			row[key] = 0  # ENHANCE: handle the null vals
		row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

	ret = f"""
	   {row['tractid']},            -- tractid
	   '{row['State']}',                -- state
	   '{row['county']}',               -- county
	   {row['totalpop']},               -- totalpop
	   {row['men']},                    -- men
	   {row['women']},                  -- women
	   {row['hispanic']},               -- hispanic
	   {row['white']},                  -- white
	   {row['black']},                  -- black
	   {row['native']},                 -- native
	   {row['asian']},                  -- asian
	   {row['pacific']},                -- pacific
	   {row['votingagecitizen']},                -- votingagecitizen
	   {row['income']},                 -- income
	   {row['incomeerr']},              -- incomeerr
	   {row['incomepercap']},           -- incomepercap
	   {row['incomepercaperr']},        -- incomepercaperr
	   {row['poverty']},                -- poverty
	   {row['childpoverty']},           -- childpoverty
	   {row['professional']},           -- professional
	   {row['service']},                -- service
	   {row['office']},                 -- office
	   {row['construction']},           -- construction
	   {row['production']},             -- production
	   {row['drive']},                  -- drive
	   {row['carpool']},                -- carpool
	   {row['transit']},                -- transit
	   {row['walk']},                   -- walk
	   {row['othertransp']},            -- othertransp
	   {row['workathome']},             -- workathome
	   {row['meancommute']},            -- meancommute
	   {row['employed']},               -- employed
	   {row['privatework']},            -- privatework
	   {row['publicwork']},             -- publicwork
	   {row['selfemployed']},           -- selfemployed
	   {row['familywork']},             -- familywork
	   {row['unemployment']}            -- unemployment
	"""

	return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

# read the input data file into a list of row strings
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	with open(fname, mode="r") as fil:
		dr = csv.DictReader(fil)
		
		rowlist = []
		for row in dr:
			rowlist.append(row)

	return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
	cmdlist = []
	for row in rowlist:
		valstr = row2vals(row)
		cmd = f"INSERT INTO {TableName} VALUES ({valstr});"
		cmdlist.append(cmd)
	return cmdlist

# connect to the database
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database="postgres",
		user="postgres",
		password="Lungsta01.",
	)
	connection.autocommit = False
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TableName};
			CREATE TABLE {TableName} (
				tractid         NUMERIC,
				state               TEXT,
				county              TEXT,
				totalpop            INTEGER,
				men                 INTEGER,
				women               INTEGER,
				hispanic            DECIMAL,
				white               DECIMAL,
				black               DECIMAL,
				native              DECIMAL,
				asian               DECIMAL,
				pacific             DECIMAL,
				votingagecitizen             DECIMAL,
				income              DECIMAL,
				incomeerr           DECIMAL,
				incomepercap        DECIMAL,
				incomepercaperr     DECIMAL,
				poverty             DECIMAL,
				childpoverty        DECIMAL,
				professional        DECIMAL,
				service             DECIMAL,
				office              DECIMAL,
				construction        DECIMAL,
				production          DECIMAL,
				drive               DECIMAL,
				carpool             DECIMAL,
				transit             DECIMAL,
				walk                DECIMAL,
				othertransp         DECIMAL,
				workathome          DECIMAL,
				meancommute         DECIMAL,
				employed            INTEGER,
				privatework         DECIMAL,
				publicwork          DECIMAL,
				selfemployed        DECIMAL,
				familywork          DECIMAL,
				unemployment        DECIMAL
			);	
			
		""")

		print(f"Created {TableName}")


def load(conn, cmdlist):
  columns = [
      'TractId', 'State', 'County', 'TotalPop', 'Men', 'Women',
      'Hispanic', 'White', 'Black', 'Native', 'Asian', 'Pacific',
      'VotingAgeCitizen', 'Income', 'IncomeErr', 'IncomePerCap', 'IncomePerCapErr',
      'Poverty', 'ChildPoverty', 'Professional', 'Service', 'Office',
      'Construction', 'Production', 'Drive', 'Carpool', 'Transit', 'Walk',
      'OtherTransp', 'WorkAtHome', 'MeanCommute', 'Employed', 'PrivateWork',
      'PublicWork', 'SelfEmployed', 'FamilyWork', 'Unemployment'
  ]
  columns2 = [x.lower() for x in columns]
  f = io.StringIO()
  writer = csv.writer(f)
  for row in cmdlist:
      for key in row:
          if not row[key]:
              row[key] = '0'
      row['County'] = row['County'].replace("'", "")
      writer.writerow([row[col] for col in columns])
  f.seek(0)
  start =  time.perf_counter()
  with conn.cursor() as cursor:
     cursor.copy_from(f, TableName, sep=',', columns = columns2)
  elapsed = time.perf_counter() - start
  print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')
  

def cons_index(conn):
  with conn.cursor() as cursor:
      cursor.execute(f"""
        ALTER TABLE {TableName} ADD PRIMARY KEY (TractId);
        CREATE INDEX idx_{TableName}_State ON {TableName}(State);
        """)
      print(f"Constraints added to {TableName}")
def main():
	initialize()
	conn = dbconnect()
	rlis = readdata(Datafile)
	#cmdlist = getSQLcmnds(rlis)

	if CreateDB:
		createTable(conn)
		conn.commit()

	load(conn, rlis)
	conn.commit()
	cons_index(conn)
	conn.commit()
	
	
	

if __name__ == "__main__":
	main()



