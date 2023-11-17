from sqlalchemy import create_engine, text

class DBConnection:

	def __init__(self):
		self.engine = create_engine('mysql+pymysql://root:root@localhost/sales_db')

	def __del__(self):
		self.engine.dispose()

	def load_stage(self, data):
		data.to_sql(con = self.engine, name = 'stage', if_exists='replace')

	def execute_script(self, filename):
	    # open and read the file as a single buffer
	    fd = open(filename, 'r')
	    sql_file = fd.read()
	    fd.close()

	    # all SQL commands (split on ';')
	    sql_commands = sql_file.split(';')

	    # Execute every command from the input file
	    with self.engine.connect() as conn:
		    for command in sql_commands:
		    	command = command.strip()
		    	if command:
		    		result = conn.execute(text(command))
			    	conn.commit()

	def get_query_result(self, filename):
	    # open and read the file as a single buffer
	    fd = open(filename, 'r')
	    sql_file = fd.read()
	    fd.close()

	    # all SQL commands (split on ';')
	    command = sql_file.strip()

	    # Execute every command from the input file
	    with self.engine.connect() as conn:
    		result = conn.execute(text(command))
	    	id_count = result.first()[0]
	    	return id_count