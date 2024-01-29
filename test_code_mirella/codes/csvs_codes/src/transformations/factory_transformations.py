from transformations.cleaning import Cleaning
from transformations.enrichment import Enrichment


class FactoryTransformations:

    def clean_sales_dataframe(self, df):
        clean = Cleaning()
        not_null_df = clean.clean_null_data(df=df)
        return clean.clean_duplicated_data(df=not_null_df)

    def enrich_sales_dataframe(self, df):
        enrich = Enrichment()
        return enrich.enrich_dollar_values(df=df)
