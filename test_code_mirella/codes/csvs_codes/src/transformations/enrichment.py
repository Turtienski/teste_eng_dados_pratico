from pyspark.sql.functions import lit, col


class Enrichment:

    def enrich_dollar_values(self, df):
        dollar_df = df.withColumn('usd_sale_value', lit(col('sale_value') * 0.75))
        return dollar_df
