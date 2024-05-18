from pyspark.sql import SparkSession, functions as sparkFunctions
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import findspark 
import os
import sys

findspark.init()
CUSTOMER_RATING_INDEX = 1
DATE_RATING_INDEX=2
AMAZON_DW_TABLES = "assets/dw-tables/amazon/"
AMAZON_EXTRACTED_DATA = "assets/data-extracted/amazon/"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class AmazonDWTablesFactorer :

    def __init__(self) -> None:
        self.spark_session = SparkSession.builder \
                            .appName("Netflix Data Integration") \
                            .config("spark.driver.memory", "4g") \
                            .getOrCreate()

    def getMovieDimension(self) ->SparkDataFrame:
            
            rating_data = self.getFactTable()
            movie_dimension = rating_data.select(["product_id", "product_title"])\
                                .withColumn("hosted_at", sparkFunctions.lit("AMAZON"))\
                                .withColumn("launch_date", sparkFunctions.lit(None).cast(DateType()))
        
           

            return movie_dimension

    def getTimeDimension(self) -> SparkDataFrame:
        rating_data = self.getFactTable()

        
        time_dimension = rating_data.withColumn("day", sparkFunctions.dayofmonth(sparkFunctions.col("rating_date"))) \
                                    .withColumn("month", sparkFunctions.month(sparkFunctions.col("rating_date"))) \
                                    .withColumn("year", sparkFunctions.year(sparkFunctions.col("rating_date")))\
                                    .withColumnRenamed("rating_date", "date")\
                                    .drop("customer_id", "product_title", "rating","product_id")
                                    

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
                            
                            
        return client_dimension


    def getFactTable(self) -> SparkDataFrame:
        rating_data = self.spark_session.read.parquet(AMAZON_EXTRACTED_DATA + "videos_review*/")\
                    .select(["customer_id","product_title", "product_id","star_rating", "review_date"])\
                    .withColumn("star_rating", sparkFunctions.col("star_rating").cast("float"))\
                    .withColumn("customer_id", sparkFunctions.expr("concat('AMA', customer_id)") )\
                    .withColumnRenamed("star_rating", "rating")\
                    .withColumnRenamed("review_date", "rating_date")\
                    .withColumn("rating_date", sparkFunctions.to_date(sparkFunctions.col("rating_date"), "yyyy-MM-dd"))
       
                    
                        
        
        return rating_data






if __name__ == "__main__":
    table_factorer = AmazonDWTablesFactorer()

    #get DW tables
    movie_dimension  = table_factorer.getMovieDimension()
    time_dimension = table_factorer.getTimeDimension()
    fact_table = table_factorer.getFactTable()

    #save tables

    movie_dimension.write.mode("overwrite").parquet(AMAZON_DW_TABLES + "movie_dimension")
    time_dimension.write.mode("overwrite").parquet(AMAZON_DW_TABLES + "time_dimension")
    fact_table.write.mode("overwrite").parquet(AMAZON_DW_TABLES + "fact_table")