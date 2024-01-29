class Write:

    def save_mysql_table(self, df, table_name):
        mysql_url = "jdbc:postgresql://localhost:5432/itau_data_egineer_test"

        properties = {
            "user": "spark",
            "password": "root",
            "driver": "org.postgresql.Driver"
        }

        df.write.jdbc(url=mysql_url, table=table_name, mode="append", properties=properties)
