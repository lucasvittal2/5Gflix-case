from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType



AMAZON_DW_TABLE_PATH = "assets/dw-tables/amazon/"
NETFLIX_DW_TABLE_PATH = "assets/dw-tables/netflix/"
DATA_INTEGRATED_PATH = "assets/data-integrated/"

class DataIntegrator:
    """
    Descrição:
        classe responsavel por integrar tabelas dw fabricadas eliminar duplicatas e valores nullos nas chaves
    """

    def __init__(self) -> None:
        """
        Descrição:
            Inicialização aplicação que faz integração dos dados
        """
        self.spark_session = SparkSession.builder.\
                                appName("Data Integration")\
                                .config("spark.driver.memory", "4g")\
                                .getOrCreate()

    def integrateMovieDimensionTables(self) -> SparkDataFrame:
        """
            Descrição:
                função que realiza integração tabelas fabricadas para dimensão filme vindos da amazon e da  netflix.
                Além da integração essa função realiza eliminação de duplicatas e valores nulos no campo designado
                para a chave primária.
            
            Saída:
                Dataframe com dados integrados para dimensão filmes

        """
        

        amazon_movie_dimension = self.spark_session.read.parquet(AMAZON_DW_TABLE_PATH + "movie_dimension")
        netflix_movie_dimension = self.spark_session.read.parquet(NETFLIX_DW_TABLE_PATH + "movie_dimension")
        integrated_movie_dimension = amazon_movie_dimension.union(netflix_movie_dimension)
        integrated_movie_dimension = integrated_movie_dimension.orderBy("name")

        #drop nulls on primary key
        integrated_movie_dimension = integrated_movie_dimension.filter(integrated_movie_dimension.id.isNotNull())

        return integrated_movie_dimension
    
    def integrateTimeDimensionTables(self) -> SparkDataFrame:
        """
            Descrição:
                função que realiza integração tabelas fabricadas para dimensão tempo vindos da amazon e da  netflix.
                Além da integração essa função realiza eliminação de duplicatas e valores nulos no campo designado
                para a chave primária.
            
            Saída:
                Dataframe com dados integrados para dimensão tempo

        """

        amazon_time_dimension = self.spark_session.read.parquet(AMAZON_DW_TABLE_PATH + "time_dimension")
        netflix_time_dimension = self.spark_session.read.parquet(NETFLIX_DW_TABLE_PATH + "time_dimension")
        integrated_time_dimension = amazon_time_dimension.union(netflix_time_dimension)

        

        #drop duplicated dates
        integrated_time_dimension = integrated_time_dimension.drop_duplicates(['date'])

        #drop nulls on primary key
        integrated_time_dimension = integrated_time_dimension.filter(integrated_time_dimension.date.isNotNull())

        return integrated_time_dimension
    
    def integrateFactTable(self):
        """
            Descrição:
                função que realiza integração tabelas fabricadas para a fato  vindos da amazon e da  netflix.
                Além da integração essa função realiza eliminação de duplicatas e valores nulos no campo designado
                para a chave primária.
            
            Saída:
                Dataframe com dados integrados para a tabela fato

        """
        amazon_fact_table = self.spark_session.read.parquet(AMAZON_DW_TABLE_PATH + "fact_table")
        netflix_fact_table = self.spark_session.read.parquet(NETFLIX_DW_TABLE_PATH + "fact_table")
        integrated_fact_table = amazon_fact_table.union(netflix_fact_table)

        #drop nulls on foreign keys
        integrated_fact_table = integrated_fact_table.filter(integrated_fact_table.movie_id.isNotNull())
        integrated_fact_table = integrated_fact_table.filter(integrated_fact_table.rating_date.isNotNull())
        integrated_fact_table = integrated_fact_table.filter(integrated_fact_table.client_id.isNotNull())

        return integrated_fact_table

if __name__ == "__main__":
    """
    Descrição:
        Implementação da integração de dados das tabelas do DW fabricadas a partir de dados da netflixe amazon.
        Pr fim, o salvamento em assets/dw-data-integrated/amazon/ em .parquet
    
    Saída: 
        Este processo gera como saida arquivos .parquet
    """
    data_integration = DataIntegrator()

    #perform and load table integrations
    movie_dimension = data_integration.integrateMovieDimensionTables()
    time_dimension = data_integration.integrateTimeDimensionTables()
    fact_table = data_integration.integrateFactTable()
    
    #save ready tables  to be load on DW
    movie_dimension.write.mode("overwrite").parquet(DATA_INTEGRATED_PATH + "movie_dimension")
    time_dimension.write.mode("overwrite").parquet(DATA_INTEGRATED_PATH + "time_dimension")
    fact_table.write.mode("overwrite").parquet(DATA_INTEGRATED_PATH + "fact_table")


    