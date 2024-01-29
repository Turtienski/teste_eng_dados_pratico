from analysis.sales_analysis import SalesAnalysis
from infrastructure.read_files.read import Read
from infrastructure.sessions.spark_session import get_spark_session


def main():
    spark = get_spark_session()
    read = Read(spark_session=spark)
    analysis = SalesAnalysis()
    wb_logs_csv = read.read_csv_file(path_name='../../../../website_logs.csv', sep=',')

    analysis.pages_visits_rank(df=wb_logs_csv).show()

    analysis.session_average(df=wb_logs_csv).show()

    analysis.weekly_visitors(df=wb_logs_csv).show()


if __name__ == '__main__':
    main()
