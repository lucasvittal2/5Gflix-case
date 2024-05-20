from typing import List
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import split, trim, col, when, length, regexp_replace, last
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, FloatType, StringType, DateType
import findspark 
import os
import sys

findspark.init()
AMAZON_DATASOURCE = "assets/source-databases/amazon/"
AMAZON_EXTRACTED_DATA = "assets/data-extracted/amazon/"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


class AmazonDataExtractor:
        """
        Descrição:
                Classe reponsável por  realizar extração de dados vindos da plataforma Amazon
        
        """
        

        def __init__(self) -> None:
                """
                Descrição:
                        Inicialização aplicação spark que realiza extração de dados
                """
                
                self.spark_session = SparkSession.builder\
                                .config("spark.driver.memory", "4g")\
                                .appName("Amazon Data Extraction").getOrCreate()

        def extractVideoReviews(self) -> SparkDataFrame:
           """
           Descrição:
                Função que realiza extração de todo os dados de filmes\series para um dataframe do spark

                Saída:
                        Dataframe com todos os dados de filmes e séries da amazon
           """
            
           video_reviews = self.spark_session.read.csv(AMAZON_DATASOURCE + "*.tsv", sep="\t", header=True, inferSchema=True)
           return  video_reviews
        

if __name__ == "__main__":
      """
      Descrição:
                Implementação de extração de dados filmes e séries  da amazon

        Saída:
                Este procedimento fornece como saída arquivos no formato .parquet que são salvos em 'assets/data-extracted/amazon/
      """
      data_extractor = AmazonDataExtractor()
      video_reviews_df = data_extractor.extractVideoReviews()
      video_reviews_df.write.mode("overwrite").parquet(AMAZON_EXTRACTED_DATA + "videos_review")