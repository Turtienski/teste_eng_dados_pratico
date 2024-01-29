from infrastructure.read_files.read import Read
from infrastructure.sessions.spark_session import get_spark_session
from infrastructure.write_files.write import Write
from transformations.factory_transformations import FactoryTransformations


def main():
    spark = get_spark_session()
    read = Read(spark_session=spark)
    write = Write()
    sales_csv = read.read_csv_file(path_name='../../../../sales_data.csv', sep=',')
    transformations = FactoryTransformations()

    cleaned_df = transformations.clean_sales_dataframe(df=sales_csv)
    cleaned_df.show()

    enriched_df = transformations.enrich_sales_dataframe(df=cleaned_df)
    enriched_df.show()

    write.save_mysql_table(df=enriched_df, table_name='sales')


if __name__ == '__main__':
    main()
