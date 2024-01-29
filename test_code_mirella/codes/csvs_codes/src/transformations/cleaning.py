from pyspark.sql.functions import col


class Cleaning:

    def clean_null_data(self, df):
        cleaned_df = df.filter(
            col('transaction_id').isNotNull() & col('date').isNotNull() &
            col('product_id').isNotNull() & col('seller_id').isNotNull() &
            col('sale_value').isNotNull() & col('currency').isNotNull()
        )
        return cleaned_df

    def clean_duplicated_data(self, df):
        return df.dropDuplicates()
