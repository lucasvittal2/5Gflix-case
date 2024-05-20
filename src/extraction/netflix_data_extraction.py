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
NETFLIX_DATASOURCE = "assets/source-databases/netflix/"
NETFLIX_EXTRACTED_DATA = "assets/data-extracted/netflix/"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



class NetflixDataExtractor:
        """
        Descrição:
                Classe reponsável por  realizar extração de dados vindos da plataforma Netflix
        
        """

        def __init__(self) -> None:
                """
                Descrição:
                        Inicialização aplicação spark que realiza extração de dados
                """
                self.spark_session = SparkSession.builder.appName("Netflix Data Extraction").getOrCreate()
                
        
        def extractMovieData(self, csv_path:str)->SparkDataFrame:
                """
                Descrição:
                        Função que realiza extração de todo os dados de filmes\series para um dataframe do spark

                Saída:
                        Dataframe com todos os dados de filmes e séries da netflix
                """
        
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
        def saveAllExtractedDataToParquet(dfs: List[SparkDataFrame], source_paths: List[str], source_type: str = ".txt") -> None: 
                """
                Argumentos:
                        dfs (List[SparkDataFrame]): Lista de DataFrames que contêm informações de filmes e avaliações extraídas.
                        source_paths (List[str]): Lista de caminhos dos arquivos de origem dos quais os dados foram extraídos.
                        source_type (str, opcional): Tipo do arquivo de origem. Por padrão, é ".txt".

                Descrição:
                        Função que realiza o salvamento de dados particionados de avaliações da Netflix. Para cada DataFrame e caminho de origem,
                        a função cria um nome de arquivo baseado no caminho de origem, substituindo o nome do arquivo de origem de "combined_data_" 
                        por "rating_data_". Em seguida, salva o DataFrame correspondente como um arquivo Parquet no diretório 'assets/data-extracted/netflix/'.

                Saída:
                        Não retorna nada. Como resultado da execução da função, os arquivos de partições extraídos são salvos no formato Parquet 
                        no diretório 'assets/data-extracted/netflix/'.
                """
                
                for df, source_path in zip(dfs, source_paths):
                        
                        
                        file_name = source_path.split("/")[-1]\
                                .replace(source_type,"")\
                                .replace("combined_data_", "rating_data_")
                        
                        print(f"Saving {file_name}...")
                        path_to_save = NETFLIX_EXTRACTED_DATA + file_name
                        df.write.mode("overwrite").parquet(path_to_save)
                        print(f"Saved on {path_to_save} !\n\n")
        
        def extractRatingData(self, txt_path: str) -> SparkDataFrame:
                """
                Argumento:
                        txt_path: Caminho de um arquivo texto relativo a dados de avaliação de filmes
                Descrição:
                        Função que realiza todo o pipeline de extração de uma partição de dados de avaliações
                        para um dataframe do spark

                Saída:
                        Dataframe com dados de avaliação de uma partição
                        
                """

                df = self.spark_session.read.text(txt_path).withColumnRenamed('value', 'line')
                df = self.__getRecords(df)
                df = self.__matchRecordsWithCorrespondentMovieId(df)
                df = self.__standardizeSchema(df)
                return df
                
        @staticmethod
        def __getRecords(df :SparkDataFrame)->SparkDataFrame:
                """
                Argumento:
                        df: Dataframe com um campo referente ao identificador do filmes e outro campo reperente as linhas.
                        os dados carregados em cada campos estam exatamente como no arquivo texto:
                                line      
                                <movie_id1>: 
                                <client_i1d>,<rating1>,<rating_date1>
                                <movie_id2>: 
                                <client_2id>,<rating2>,<rating_date2>
                                                .
                                                .
                                                .
                Descrição:
                        Função que realiza captura  identificador de filmes faz a limpez desse campo e
                        separa campos  <client_id>,<rating>,<rating_date>.




                Saída:
                        Dataframe com dados de indentificador de filmes limpos e dados de avaliação separados
                        em seus respetivos campos poŕem desconectados:
                                                                movie_id	parts
                                movie_id1   |	null
                                null	    |	[client_id1, rating1, rating_date1]
                                null	    |	[client_id2, rating2, rating_date2]
                                movie_id2   |	null
                                null	    |	[client_id3, rating3, rating_date3]
                        
                """

                df = df.withColumn('movie_id', when(col('line').endswith(':'), trim(regexp_replace(col('line'), ':', ''))))
                df = df.withColumn('parts', when(~col('line').endswith(':'), split(col('line'), ',')))
                return df
        
        @staticmethod
        def __matchRecordsWithCorrespondentMovieId(df: SparkDataFrame):
                """
                Argumentos:
                        df (SparkDataFrame): Dataframe com dados de indentificador de filmes limpos e dados de avaliação separados
                        em seus respetivos campos.

                Descrição:
                        Função estática que associa cada registro com o identificador de filme correspondente. 
                        Utiliza uma janela de dados que começa desde o início do DataFrame até a linha atual (unboundedPreceding até currentRow). 
                        Para cada linha na janela, a função preenche a coluna 'movie_id' com o último valor não nulo encontrado na coluna 'movie_id'.

                Saída:
                        Retorna o DataFrame modificado com identificadores de filmes associados as suas respectivas avaliações
                                                                movie_id	parts
                                        movie_id1   |	[client_id1, rating1, rating_date1]
                                        movie_id1   |	[client_id1, rating1, rating_date1]
                                        movie_id1   |	[client_id2, rating2, rating_date2]
                                        movie_id2   |	[client_id3, rating3, rating_date3]
                                        movie_id3   |	[client_id3, rating3, rating_date3]
                                        .
                                        .
                                        .

                """
                window_spec = Window.orderBy().rowsBetween(Window.unboundedPreceding, Window.currentRow)
                df = df.withColumn('movie_id', last('movie_id', True).over(window_spec))
                return df
        
        def __standardizeSchema(self, df):
                """
                Argumentos:
                        df (SparkDataFrame): Dataframe com dados de indentificador de filmes limpos mapeados as lista com dados de avaliações
                        em seus respetivos campos.

                Descrição:
                        Função estática que mapeia registros de avaliações aos seus respectivos campos.

                Saída:
                        Retorna o DataFrame modificado com identificadores de filmes com sess respectivos dados de avalação mapeados nos seus devidos campos.
                """     
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
        """
        Descrição:
                Implementação de extração de dados filmes e séries  da netflix

        Saída:
                Este procedimento fornece como saída arquivos no formato .parquet que são salvos em 'assets/data-extracted/netflix/
      """

        #initialize extraction App
        data_extractor = NetflixDataExtractor()

        #load paths
        files = os.listdir(NETFLIX_DATASOURCE)
        csv_paths = [NETFLIX_DATASOURCE + csv_file for csv_file in files if "movie_titles" in csv_file]
        txt_paths = [NETFLIX_DATASOURCE + txt_file for txt_file in files if "combined_data_" in txt_file]
        
        
        #extracting rating paths
        rating_dfs  = [ data_extractor.extractRatingData(txt_path) for txt_path in txt_paths ]

        #extract movie data
        movie_dfs = [ data_extractor.extractMovieData(csv_path) for csv_path in csv_paths ]

        #save all data
        data_extractor.saveAllExtractedDataToParquet(rating_dfs, txt_paths, source_type=".txt")
        data_extractor.saveAllExtractedDataToParquet(movie_dfs, csv_paths, source_type=".csv")
