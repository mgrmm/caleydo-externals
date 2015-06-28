

class SQLProvider(object):
	def __init__(self):
		#import caleydo.config
		#c = caleydo.config.view('sql-dataset')
		import sqlite3
		import numpy
		self.conn = sqlite3.connect('sl1.db')
		
		self.conn.execute("CREATE TABLE IF NOT EXISTS Tables (Table_ID integer primary key, Name varchar(30), Id_Type varchar(30))")
		self.conn.execute("CREATE TABLE IF NOT EXISTS Columns (Column_ID integer primary key, Column_Name varchar(30), Table_ID integer, Value_Type varchar(30), Size varchar(30), FOREIGN Key(Table_ID) references Tables(Table_ID))")
		self.conn.execute("CREATE TABLE IF NOT EXISTS Table_Data (Value_ID integer primary key, Column_ID integer, Row integer, Value varachar(30), FOREIGN Key(Column_ID) references Columns(Column_ID))")
		self.conn.execute("CREATE TABLE IF NOT EXISTS Matrix (Matrix_ID integer primary key, Name varchar(30), Column_Type varchar(30), Value_Type varchar(30), Row_Type varchar(30), Size varchar(30), Range varchar(30))") 
		self.conn.execute("CREATE TABLE IF NOT EXISTS Matrix_Data (Value_ID integer primary key, Row varachar(30), Column varachar(30), Value varachar(30), Matrix_ID integer, FOREIGN Key(Matrix_ID) references Matrices(Matrix_ID))")
		
		self.list = []
		matrices = self.conn.execute("SELECT * FROM Matrix")
		for matrix in matrices:
			entry["id"] = matrix[0]
			entry["name"] = matrix[1]
			entry["coltype"] = matrix[2]
			entry["type"] = matrix[3]
			entry["rowtype"] = matrix[4]
			entry["size"] = matrix[5]
			entry["range"] = matrix[6]
			entry["dtype"] = "matrix"
			rows = self.conn.execute("SELECT * FROM Matrix_Data WHERE Matrix_ID = " + matrix[0])
			entry["data"] = numpy.zeros(shape = entry["size"])
			for row in rows:
				entry["data"][row[1]][row[2]] = row[3]
			self.list.append(entry)
		tables = self.conn.execute("SELECT * FROM Tables")
		for table in tables:
			entry["id"] = table[0]
			entry["name"] = table[1]
			entry["idtype"] = table[2]
			entry["dtype"] = "table"
			columnDescription = self.conn.execute("SELECT * FROM Columns WHERE Table_ID= " + table[0])
			column["name"] = columnDescription[1]
			column["type"] = columnDescription[3]
			column["type"] = columnDescription[4]
			column["data"] = numpy.zeros(shape = entry["size"])
			cols = self.conn.execute("SELECT * FROM Table_Data WHERE Column_ID = " + columnDescription[0])
			for col in cols:
				column["data"][col[2]] = col[3]
			self.list.append(entry)
		self.conn.close()
			
	def __len__(self):
		return sum((len(f) for f in self.files))

	def __iter__(self):
		return itertools.chain(*self.files)
		

if __name__ == '__main__':
  # app.debug1 = True

  c = SQLProvider()
 # l = c.list()
 # print l
 # print c.idtypes()
 # for li in l:
  #  c.get(li)


def create():
  return SQLProvider()