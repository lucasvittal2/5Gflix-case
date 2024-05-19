from snowflake_handling import *




class SnowflakeDataLoader:

    def __init__(self, plataform_handler: DataPlaformHandler) -> None:

        #instantiate secret variables
        self.database = os.getenv("DATABASE_NAME")
        self.aws_secret_key = os.getenv("AWS_SECRET_KEY")
        self.aws_acces_key = os.getenv("AWS_ACCESS_KEY")
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        database_name = os.getenv("DATABASE_NAME")
        
        # point to the stagging schema
        self.plataform_handler = plataform_handler
        self.plataform_handler.runQuery("CREATE SCHEMA IF NOT EXISTS STAGGING")
        self.plataform_handler.runQuery(f"USE {database_name}.STAGGING")

    def loadMovieDimensionToDataWarehouse(self) -> None:

        query = f"""
                CREATE STAGE IF NOT EXISTS STAGGING.stg_movie_dimension 
                URL = 's3://{self.bucket_name}/movie_dimension/' 
                CREDENTIALS = ( AWS_KEY_ID = '{self.aws_acces_key}' AWS_SECRET_KEY = '{self.aws_secret_key}' ) 
                DIRECTORY = ( ENABLE = true ) 
                COMMENT = 'movie dimension stagging';
            """
        self.plataform_handler.runQuery(query)

        
        

    def loadTimeDimensionToDataWarehouse(self) -> None:

        query = f"""
                CREATE STAGE IF NOT EXISTS STAGGING.stg_time_dimension 
                URL = 's3://{self.bucket_name}/time_dimension/' 
                CREDENTIALS = ( AWS_KEY_ID = '{self.aws_acces_key}' AWS_SECRET_KEY = '{self.aws_secret_key}' ) 
                DIRECTORY = ( ENABLE = true ) 
                COMMENT = 'time dimension stagging';
            """
        self.plataform_handler.runQuery(query)

    def loadFactTableToDataWarehouse(self) -> None:
        query = f"""
                CREATE STAGE IF NOT EXISTS STAGGING.stg_fact_table 
                URL = 's3://{self.bucket_name}/fact_table/' 
                CREDENTIALS = ( AWS_KEY_ID = '{self.aws_acces_key}' AWS_SECRET_KEY = '{self.aws_secret_key}' ) 
                DIRECTORY = ( ENABLE = true ) 
                COMMENT = 'fact_table stagging';
            """
        self.plataform_handler.runQuery(query)


if __name__ == "__main__":
        
        snowflake_handler = SnowflakeDataPlataformHandler()
        data_loader = SnowflakeDataLoader(plataform_handler=SnowflakeDataPlataformHandler())

        print("loading Time Dimension...")
        data_loader.loadTimeDimensionToDataWarehouse()
        print("Time Dimension Loaded !\n\n")

        print("loading Movie Dimension...")
        data_loader.loadMovieDimensionToDataWarehouse()
        print("Movie Dimension Loaded !\n\n")

        print("loading Fact Table...")
        data_loader.loadFactTableToDataWarehouse()
        print("Fact Table loaded !")