from pyspark.sql import SparkSession, functions as sparkFunctions

from typing import List
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import findspark 
import os
import sys

findspark.init()
CUSTOMER_RATING_INDEX = 1
DATE_RATING_INDEX=2
NETFLIX_DW_TABLES = "assets/dw-tables/netflix/"
NETFLIX_EXTRACTED_DATA = "assets/data-extracted/netflix/"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class NetFlixDataIntegrator :

    def __init__(self) -> None:
        self.spark_session = SparkSession.builder \
                            .appName("Netflix Data Integration") \
                            .config("spark.driver.memory", "4g") \
                            .getOrCreate()


    
        
    
    def getMovieDimension(self) ->SparkDataFrame:
        movie_dimension = self.spark_session.read.parquet(NETFLIX_EXTRACTED_DATA + "movie_titles*/")
        movie_dimension = movie_dimension.withColumnRenamed("year", "launch_year")\
                                .withColumnRenamed("movie_id", "id")\
                                .withColumnRenamed("title", "name")
        
        # drop duplicates and order by launch year
        movie_dimension = movie_dimension.drop_duplicates(["id"]).orderBy("launch_year")

        return movie_dimension

    def getTimeDimension(self) -> SparkDataFrame:
        rating_data = self.getFactTable()

        
        time_dimension = rating_data.withColumn("day", sparkFunctions.dayofmonth(sparkFunctions.col("rating_date"))) \
                                    .withColumn("month", sparkFunctions.month(sparkFunctions.col("rating_date"))) \
                                    .withColumn("year", sparkFunctions.year(sparkFunctions.col("rating_date")))\
                                    .withColumnRenamed("rating_date", "date")\
                                    .drop("movie_id", "client_id", "rating")
                                    

        #drop duplicates and order by date
        time_dimension = time_dimension.drop_duplicates(["date"]).orderBy("date")

        return time_dimension

    def getClientDimension(self)-> SparkDataFrame:
        rating_data = self.getFactTable()
        client_dimension  = rating_data.select("client_id")\
                            .withColumnRenamed("client_id", "id")\
                            .drop_duplicates(["id"])\
                            .withColumn("name", sparkFunctions.expr("concat('client ', id)"))\
                            .withColumn("subscribed_at", sparkFunctions.lit("NETFLIX"))\
                            .withColumn("id", sparkFunctions.expr("concat('NET', id)") )
                            
        return client_dimension


    def getFactTable(self) -> SparkDataFrame:
        rating_data = self.spark_session.read.parquet(NETFLIX_EXTRACTED_DATA + "rating_data*/")\
                            .withColumn("rating_date", sparkFunctions.to_date(sparkFunctions.col("rating_date"), "yyyy-MM-dd"))
        
        return rating_data
    
    

if __name__ == "__main__":

    data_integrator = NetFlixDataIntegrator()

    # get tables of DW
    time_dimension_df= data_integrator.getTimeDimension()
    movie_dimension_df = data_integrator.getMovieDimension()
    client_dimension_df = data_integrator.getClientDimension()
    fact_df = data_integrator.getFactTable()


    
    #save for checkpoint

    time_dimension_df.write.mode("overwrite").parquet(NETFLIX_DW_TABLES + "time_dimension")
    movie_dimension_df.write.mode("overwrite").parquet(NETFLIX_DW_TABLES + "movie_dimension")
    client_dimension_df.write.mode("overwrite").parquet(NETFLIX_DW_TABLES + "client_dimension")
    fact_df.write.mode("overwrite").parquet(NETFLIX_DW_TABLES + "fact_table")