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


class NetflixDWTablesFactorer :
    """
    Descrição:
        classe responsavel por criar tabelas do DW com esquema padronizado com extraidos dados da netflix
    """

    def __init__(self) -> None:
        """
        Descrição:
            Inicialização da aplicação spark que realiza fabricação das tabelas para DW com dados extraidos da netflix

        """
        self.spark_session = SparkSession.builder \
                            .appName("Netflix Dw tables manufactoring") \
                            .config("spark.driver.memory", "4g") \
                            .getOrCreate()


    
        
    
    def getMovieDimension(self) ->SparkDataFrame:
        """
        Descrição:
            função que fabrica tabela da dimensão filme com dados da netflix:
        Saída:
            Tabela dimensão filme no formato Dataframe

        """
        movie_dimension = self.spark_session.read.parquet(NETFLIX_EXTRACTED_DATA + "movie_data*/")
        movie_dimension = movie_dimension.withColumnRenamed("year", "launch_year")\
                                .withColumn("hosted_at", sparkFunctions.lit("NETFLIX"))\
                                .withColumnRenamed("movie_id", "id")\
                                .withColumnRenamed("title", "name")\
                                .select(["id", "name", "launch_year", "hosted_at"])
        
        # drop duplicates and order by launch year
        movie_dimension = movie_dimension.drop_duplicates(["id"]).orderBy("launch_year")

        return movie_dimension

    def getTimeDimension(self) -> SparkDataFrame:
        """
            Descrição:
                função que fabrica tabela da dimensão tempo com dados da netflix:
            Saída:
                Tabela dimensão tempo no formato Dataframe

        """
        rating_data = self.spark_session.read.parquet(NETFLIX_EXTRACTED_DATA + "rating_data*/")
        
        time_dimension = rating_data.withColumn("day", sparkFunctions.dayofmonth(sparkFunctions.col("rating_date"))) \
                                    .withColumn("month", sparkFunctions.month(sparkFunctions.col("rating_date"))) \
                                    .withColumn("year", sparkFunctions.year(sparkFunctions.col("rating_date")))\
                                    .withColumnRenamed("rating_date", "date")\
                                    .drop("movie_id", "client_id", "rating")\
                                    .select(["date","day","month", "year"])
                                    

        #drop duplicates and order by date
        time_dimension = time_dimension.drop_duplicates(["date"]).orderBy("date")

        return time_dimension



    def getFactTable(self) -> SparkDataFrame:

        """
            Descrição:
                função que fabrica tabela fato com dados da netflix:
            Saída:
                Tabela fato no formato Dataframe

        """
        rating_data = self.spark_session.read.parquet(NETFLIX_EXTRACTED_DATA + "rating_data*/")\
                            .withColumn("rating_date", sparkFunctions.to_date(sparkFunctions.col("rating_date"), "yyyy-MM-dd"))\
                            .withColumn("client_id", sparkFunctions.expr("concat('NET', client_id)") )\
                            .select(["rating_date", "client_id", "movie_id", "rating"])
        
        return rating_data
    
    

if __name__ == "__main__":
    """
    Descrição:
        Implementação da fabricação das tabelas do DW e salvamento em assets/dw-tables/amazon/ em .parquet
    
    Saída: 
        Este processo gera como saida arquivos .parquet
    """
    table_factorer = NetflixDWTablesFactorer()

    # get tables of DW
    time_dimension_df= table_factorer.getTimeDimension()
    movie_dimension_df = table_factorer.getMovieDimension()
    fact_df = table_factorer.getFactTable()


    
    #save for checkpoint

    time_dimension_df.write.mode("overwrite").parquet(NETFLIX_DW_TABLES + "time_dimension")
    movie_dimension_df.write.mode("overwrite").parquet(NETFLIX_DW_TABLES + "movie_dimension")
    fact_df.write.mode("overwrite").parquet(NETFLIX_DW_TABLES + "fact_table")