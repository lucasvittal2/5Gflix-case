from pyspark.sql import SparkSession, functions as sparkFunctions
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import findspark 
import os
import sys


AMAZON_DW_TABLES = "assets/dw-tables/amazon/"
AMAZON_EXTRACTED_DATA = "assets/data-extracted/amazon/"

# set up spark locally
findspark.init()
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class AmazonDWTablesFactorer :
    """
    Descrição:
        classe responsavel por criar tabelas do DW com esquema padronizado com dados extraidos da amazon
    """

    def __init__(self) -> None:
        """
        Descrição:
            Inicialização da aplicação spark que realiza fabricação das tabelas para DW com dados extraidos da amazon

        """
        self.spark_session = SparkSession.builder \
                            .appName("Amazon DW tables manufactoring") \
                            .config("spark.driver.memory", "4g") \
                            .getOrCreate()

    def getMovieDimension(self) ->SparkDataFrame:
            """
            Descrição:
                função que fabrica tabela da dimensão filme com dados da amazon:
            Saída:
                Tabela dimensão filme no formato Dataframe

            """
            
            full_table = self.spark_session.read.parquet(AMAZON_EXTRACTED_DATA + "videos_review*/")
            movie_dimension = full_table.select(["product_id", "product_title"])\
                                .withColumn("hosted_at", sparkFunctions.lit("AMAZON"))\
                                .withColumn("launch_year", sparkFunctions.lit(None).cast(IntegerType()))\
                                .withColumnRenamed("product_id", "id")\
                                .withColumnRenamed("product_title", "name")\
                                .select(["id", "name", "launch_year", "hosted_at"])
            
            #drop movie duplicates
            movie_dimension = movie_dimension.drop_duplicates(['id'])                         
           

            return movie_dimension

    def getTimeDimension(self) -> SparkDataFrame:
        
        """
            Descrição:
                função que fabrica tabela da dimensão tempo com dados da amazon:
            Saída:
                Tabela dimensão tempo no formato Dataframe

        """
        full_table = self.spark_session.read.parquet(AMAZON_EXTRACTED_DATA + "videos_review*/")
        time_dimension = full_table.withColumnRenamed("review_date", "date")\
                                    .withColumn("day", sparkFunctions.dayofmonth(sparkFunctions.col("date"))) \
                                    .withColumn("month", sparkFunctions.month(sparkFunctions.col("date"))) \
                                    .withColumn("year", sparkFunctions.year(sparkFunctions.col("date")))\
                                    .drop("customer_id", "product_title", "rating","product_id")\
                                    .select(["date","day","month", "year"])
                                    

        #drop duplicates and order by date
        time_dimension = time_dimension.drop_duplicates(["date"]).orderBy("date")

        return time_dimension


    def getFactTable(self) -> SparkDataFrame:
        """
            Descrição:
                função que fabrica tabela fato com dados da amazon:
            Saída:
                Tabela fato no formato Dataframe

        """
        rating_data = self.spark_session.read.parquet(AMAZON_EXTRACTED_DATA + "videos_review*/")\
                    .select(["customer_id","product_title", "product_id","star_rating", "review_date"])\
                    .withColumn("rating", sparkFunctions.col("star_rating").cast("float"))\
                    .withColumn("client_id", sparkFunctions.expr("concat('AMA', customer_id)") )\
                    .withColumnRenamed("review_date", "rating_date")\
                    .withColumn("rating_date", sparkFunctions.to_date(sparkFunctions.col("rating_date"), "yyyy-MM-dd"))\
                    .withColumnRenamed("product_id", "movie_id")\
                    .select(["rating_date", "client_id", "movie_id", "rating"])
       
                    
                        
        
        return rating_data






if __name__ == "__main__":

    """
    Descrição:
        Implementação da fabricação das tabelas do DW e salvamento em assets/dw-tables/amazon/ em .parquet

    Saída: 
        Este processo gera como saida arquivos .parquet
    """
    table_factorer = AmazonDWTablesFactorer()

    #get DW tables
    movie_dimension  = table_factorer.getMovieDimension()
    time_dimension = table_factorer.getTimeDimension()
    fact_table = table_factorer.getFactTable()

    #save tables

    movie_dimension.write.mode("overwrite").parquet(AMAZON_DW_TABLES + "movie_dimension")
    time_dimension.write.mode("overwrite").parquet(AMAZON_DW_TABLES + "time_dimension")
    fact_table.write.mode("overwrite").parquet(AMAZON_DW_TABLES + "fact_table")