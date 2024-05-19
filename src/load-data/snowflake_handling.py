import os
import snowflake.connector
from typing import List



class DataPlaformHandler:

    def runQuery(self, query: str) -> List[dict] | List[tuple]:
        raise NotImplementedError("The method 'readTableFromPlataform' were inhereted but not implemeted !")
    

    
class SnowflakeDataPlataformHandler(DataPlaformHandler):

    def __init__(self) -> None:
        
        self.connection = snowflake.connector.connect(
                                                user=os.getenv("USER_NAME"),
                                                password=os.getenv("USER_PASSWORD"),
                                                account=os.getenv("USER_ACCOUNT"),
                                                database=os.getenv("DATABASE_NAME")
                                            )

    def __del__(self):
  
        self.connection.close()


    def __enter__(self):
       
        self.connection_session = self.connection.cursor()


    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
  
        self.connection_session.close()

        

    def runQuery(self, query: str) -> List[dict] | List[tuple]:

            with self:
                try:
                    self.connection_session.execute(query)
                    rows = self.connection_session.fetchall()
                    return rows
                
                except Exception as err:
                    
                    raise Exception("Failure on execute query due to following error: \n\n{err}\n\n")
                    
                    

                    
    