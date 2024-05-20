from snowflake_handling import *




class SnowflakeDataLoader:
    """
    Descrição:
        CLasse resposável por carrega dados vindos da stagging do S3 para stagging do snowflake
    """

    def __init__(self, plataform_handler: DataPlaformHandler) -> None:
        """
        Argumentos:
            plataform_handler: Instancia da asse que faz interface com SGBD de alguma plataforma.
        Descrição:
           carregamento  de parametros necessário para realizar carregamento da stagging do s3 para 
           stagging do snowflake. 
        """

        #instantiate secret variables
        self.database = os.getenv("DATABASE_NAME")
        self.schema_name=os.getenv("SCHEMA_NAME")
        self.aws_secret_key = os.getenv("AWS_SECRET_KEY")
        self.aws_acces_key = os.getenv("AWS_ACCESS_KEY")
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.database_name = os.getenv("DATABASE_NAME")
        
        # point to the stagging schema
        self.plataform_handler = plataform_handler
        

    def loadMovieDimensionToDataWarehouse(self) -> None:
        """
        Descrição:
           Método que faz carregamento de dados da dimensão filme que esta no S3 para stagging no Snowflake.
           Esta função trata exeção para caso algum evento adverso externo ao programa aconteça

        """
        try:
            print("loading Movie Dimension...")
            query = f"""
                    CREATE STAGE IF NOT EXISTS {self.database_name}.{self.schema_name}.stg_movie_dimension 
                    URL = 's3://{self.bucket_name}/movie_dimension/' 
                    CREDENTIALS = ( AWS_KEY_ID = '{self.aws_acces_key}' AWS_SECRET_KEY = '{self.aws_secret_key}' ) 
                    DIRECTORY = ( ENABLE = true ) 
                    COMMENT = 'movie dimension stagging';
                """
            self.plataform_handler.runQuery(query)
            print("Movie Dimension Loaded !\n\n")

        except Exception as err:
                print(f"ERRO: Failed to load time Dimension data into stagging.")
                print(str(err))
        

    def loadTimeDimensionToDataWarehouse(self) -> None:
        """
        Descrição:
           Método que faz carregamento de dados da dimensão tempo que esta no S3 para stagging no Snowflake.
           Esta função trata exeção para caso algum evento adverso externo ao programa aconteça

        """
        try:
            print("loading Time Dimension...")
            query = f"""
                    CREATE STAGE IF NOT EXISTS {self.database_name}.{self.schema_name}.stg_time_dimension 
                    URL = 's3://{self.bucket_name}/time_dimension/' 
                    CREDENTIALS = ( AWS_KEY_ID = '{self.aws_acces_key}' AWS_SECRET_KEY = '{self.aws_secret_key}' ) 
                    DIRECTORY = ( ENABLE = true ) 
                    COMMENT = 'time dimension stagging';
                """
            self.plataform_handler.runQuery(query)
            print("Time Dimension Loaded !\n\n")

        except Exception as err:
                print(f"ERRO: Failed to load time Dimension data into stagging.")
                print(str(err))

    def loadFactTableToDataWarehouse(self) -> None:
        """
        Descrição:
           Método que faz carregamento de dados da tabela fato que esta no S3 para stagging no Snowflake.
           Esta função trata exeção para caso algum evento adverso externo ao programa aconteça

        """
        try:
            print("loading Fact Table...")
            query = f"""
                    CREATE STAGE IF NOT EXISTS {self.database_name}.{self.schema_name}.stg_fact_table 
                    URL = 's3://{self.bucket_name}/fact_table/' 
                    CREDENTIALS = ( AWS_KEY_ID = '{self.aws_acces_key}' AWS_SECRET_KEY = '{self.aws_secret_key}' ) 
                    DIRECTORY = ( ENABLE = true ) 
                    COMMENT = 'fact_table stagging';
                """
            self.plataform_handler.runQuery(query)
            print("Fact Table loaded !")

        except Exception as err:
                print(f"ERRO: Failed to load fact table datainto stagging.")
                print(str(err))
                


if __name__ == "__main__":
        """
            Descrição:
                Implementação de carregamento de dados de dados vindos do bucket S3 para Stagging do Snowflake.
        """
        snowflake_handler = SnowflakeDataPlataformHandler()
        data_loader = SnowflakeDataLoader(plataform_handler=SnowflakeDataPlataformHandler())

        
        data_loader.loadTimeDimensionToDataWarehouse()
        data_loader.loadMovieDimensionToDataWarehouse()
        data_loader.loadFactTableToDataWarehouse()
        