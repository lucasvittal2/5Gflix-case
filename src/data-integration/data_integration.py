from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType



AMAZON_DW_TABLE_PATH = "assets/dw-tables/amazon/"
NETFLIX_DW_TABLE_PATH = "assets/dw-tables/netflix/"
DATA_INTEGRATED_PATH = "assets/data-integrated/"

class DataIntegrator:

    def __init__(self) -> None:
        self.spark_session = SparkSession.builder.\
                                appName("Data Integration")\
                                .config("spark.driver.memory", "4g")\
                                .getOrCreate()

    def integrateMovieDimensionTables(self) -> SparkDataFrame:

        

        amazon_movie_dimension = self.spark_session.read.parquet(AMAZON_DW_TABLE_PATH + "movie_dimension")
        netflix_movie_dimension = self.spark_session.read.parquet(NETFLIX_DW_TABLE_PATH + "movie_dimension")
        integrated_movie_dimension = amazon_movie_dimension.union(netflix_movie_dimension)
        integrated_movie_dimension = integrated_movie_dimension.orderBy("name")

        return integrated_movie_dimension
    
    def integrateTimeDimensionTables(self) -> SparkDataFrame:

        

        amazon_time_dimension = self.spark_session.read.parquet(AMAZON_DW_TABLE_PATH + "time_dimension")
        netflix_time_dimension = self.spark_session.read.parquet(NETFLIX_DW_TABLE_PATH + "time_dimension")
        integrated_time_dimension = amazon_time_dimension.union(netflix_time_dimension)

        #drop duplicated dates
        integrated_time_dimension = integrated_time_dimension.drop_duplicates(['date'])
        return integrated_time_dimension
    
    def integrateFactTable(self):
        amazon_fact_table = self.spark_session.read.parquet(AMAZON_DW_TABLE_PATH + "fact_table")
        netflix_fact_table = self.spark_session.read.parquet(NETFLIX_DW_TABLE_PATH + "fact_table")
        integrated_fact_table = amazon_fact_table.union(netflix_fact_table)
        return integrated_fact_table

if __name__ == "__main__":
    data_integration = DataIntegrator()

    #perform and load table integrations
    movie_dimension = data_integration.integrateMovieDimensionTables()
    time_dimension = data_integration.integrateTimeDimensionTables()
    fact_table = data_integration.integrateFactTable()
    
    #save ready tables  to be load on DW
    movie_dimension.write.mode("overwrite").parquet(DATA_INTEGRATED_PATH + "movie_dimension")
    time_dimension.write.mode("overwrite").parquet(DATA_INTEGRATED_PATH + "time_dimension")
    fact_table.write.mode("overwrite").parquet(DATA_INTEGRATED_PATH + "fact_table")
    