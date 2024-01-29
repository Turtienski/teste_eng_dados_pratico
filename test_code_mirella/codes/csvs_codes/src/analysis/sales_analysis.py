from pyspark.sql.functions import col, avg, weekofyear


class SalesAnalysis:

    def pages_visits_rank(self, df):
        grouped_df = df.groupBy(col('page_url')).count().orderBy('count', ascending=False)
        return grouped_df

    def session_average(self, df):
        enriched_df = df.agg(avg(col('session_duration')))
        return enriched_df

    def weekly_visitors(self, df):
        enriched_df = df.withColumn('visitor_date', col('date').cast('date'))\
                        .withColumn('weeks', weekofyear('visitor_date'))\
                        .groupBy('user_id', 'weeks').count()

        filtered_df = enriched_df.filter(col('count') >= 2)
        return filtered_df
