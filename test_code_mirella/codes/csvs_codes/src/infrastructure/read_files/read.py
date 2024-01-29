

class Read:

    def __init__(self, spark_session):
        self.__spark = spark_session

    def read_csv_file(self, path_name, sep):
        return self.__spark.read.csv(path=path_name, header=True, sep=sep)
