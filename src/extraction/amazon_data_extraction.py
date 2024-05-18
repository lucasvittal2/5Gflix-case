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
        

        def __init__(self) -> None:
                
                self.spark_session = SparkSession.builder\
                                .config("spark.driver.memory", "4g")\
                                .appName("Amazon Data Extraction").getOrCreate()

        def extractVideoReviews(self) -> SparkDataFrame:
            
           video_reviews = self.spark_session.read.csv(AMAZON_DATASOURCE + "*.tsv", sep="\t", header=True, inferSchema=True)
           return  video_reviews
        

if __name__ == "__main__":
      data_extractor = AmazonDataExtractor()
      video_reviews_df = data_extractor.extractVideoReviews()
      video_reviews_df.write.mode("overwrite").parquet(AMAZON_EXTRACTED_DATA + "videos_review")