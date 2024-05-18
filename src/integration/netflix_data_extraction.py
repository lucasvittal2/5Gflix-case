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
CUSTOMER_RATING_INDEX = 1
DATE_RATING_INDEX=2
NETFLIX_DATASOURCE = "assets/Sources Databases/netflix/"
NETFLIX_EXTRACTED_DATA = "assets/Data Extracted/netflix/"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



class NetflixDataExtractor:
        

        def __init__(self) -> None:
                
                self.spark_session = SparkSession.builder.appName("Netflix Data Integration").getOrCreate()
                
        @staticmethod
        def saveAllExtractedDataToParquet(dfs: List[SparkDataFrame], source_paths: List[str], source_type: str = ".txt") -> None: 
                for df, source_path in zip(dfs, source_paths):
                        
                        
                        file_name = source_path.split("/")[-1]\
                                .replace(source_type,"")\
                                .replace("combined_data_", "rating_data_")
                        
                        print(f"Saving {file_name}...")
                        path_to_save = NETFLIX_EXTRACTED_DATA + file_name
                        df.write.mode("overwrite").parquet(path_to_save)
                        print(f"Saved on {path_to_save} !\n\n")
        
        def extractRatingData(self, txt_path: str) -> SparkDataFrame:
                df = self.spark_session.read.text(txt_path).withColumnRenamed('value', 'line')
                df = self.__getRecords(df)
                df = self.__matchRecordsWithCorrespondentMovieId(df)
                df = self.__standardizeSchema(df)
                return df
        
        def extractMovieData(self, csv_path:str)->SparkDataFrame:
                # Define the schema
                schema = StructType([
                StructField("movie_id", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("title", StringType(), True)
                ])

                # Read the CSV file with the defined schema
                df = self.spark_session.read.csv(csv_path, schema=schema, header=True)
                return df
                
        @staticmethod
        def __getRecords(df :SparkDataFrame)->SparkDataFrame:
                df = df.withColumn('movie_id', when(col('line').endswith(':'), trim(regexp_replace(col('line'), ':', ''))))
                df = df.withColumn('parts', when(~col('line').endswith(':'), split(col('line'), ',')))
                return df
        
        @staticmethod
        def __matchRecordsWithCorrespondentMovieId(df: SparkDataFrame):
                window_spec = Window.orderBy().rowsBetween(Window.unboundedPreceding, Window.currentRow)
                df = df.withColumn('movie_id', last('movie_id', True).over(window_spec))
                return df
        
        def __standardizeSchema(self, df):

                df = df.filter(df.parts.isNotNull())

               
                df = df.select(
                        col('movie_id'),
                        col('parts').getItem(0).alias('client_id'),
                        col('parts').getItem(1).alias('rating'),
                        col('parts').getItem(2).alias('rating_date')
                )

                df = df.withColumn("rating", col("rating") + ".0")
                
                schema = StructType([
                        StructField("movie_id", StringType(), True),
                        StructField("client_id", StringType(), True),
                        StructField("rating", FloatType(), True),
                        StructField("rating_date", StringType(), True)
                ])

                
                final_df = self.spark_session.createDataFrame(df.rdd, schema)

                return final_df
        
        

if __name__ == "__main__":

        #initialize extraction App
        data_extractor = NetflixDataExtractor()

        #load paths
        files = os.listdir(NETFLIX_DATASOURCE)
        csv_paths = [NETFLIX_DATASOURCE + csv_file for csv_file in files if ".csv" in csv_file]
        txt_paths = [NETFLIX_DATASOURCE + txt_file for txt_file in files if ".txt" in txt_file]
        txt_paths = [txt_paths[0]]
        
        #extracting rating paths
        rating_dfs  = [ data_extractor.extractRatingData(txt_path) for txt_path in txt_paths ]

        #extract movie data
        movie_dfs = [ data_extractor.extractMovieData(csv_path) for csv_path in csv_paths ]

        #save all data
        data_extractor.saveAllExtractedDataToParquet(rating_dfs, txt_paths, source_type=".txt")
        data_extractor.saveAllExtractedDataToParquet(movie_dfs, csv_paths, source_type=".csv")
