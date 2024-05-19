import os
import snowflake.connector
from typing import List
import time 


MAX_RETRIES=5
RETRY_WAITING_TIME= 3 # in seconds


class DataPlaformHandler:

    def readTableFromPlataform(query: str) -> List[dict] | List[tuple]:
        raise NotImplementedError("The method 'readTableFromPlataform' were inhereted but not implemeted !")
    
    def writeTableOnataform(query: str) -> None:
        raise NotImplementedError("The method 'readTableFromPlataform' were inhereted but not implemeted !")
    
class SnowflakeDataPlataformHandler(DataPlaformHandler):

    def __init__(self) -> None:
        print("Connecting to snowflake database..")
        self.connection = snowflake.connector.connect(
                                                user=os.getenv("USERNAME"),
                                                password=os.getenv("USERPASSWORD"),
                                                account=os.getenv("USERACCOUNT"),
                                                database=os.getenv("DATABASENAME"),
                                                schema=os.getenv("SCHEMANAME")
                                            )
        print("connection made sucessfully ! \n\n")

    def __del__(self):
        print("Disconnection from snowflake database...")
        self.connection.close()
        print("Disconnected!")

    def __enter__(self):
        print("starting session...")
        self.connection_session = self.connection.cursor()
        print("session were started!")

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        print("endeding session...")
        self.connection_session.close()
        print("session were ended!")
        

    def readTableFromPlataform(self, query: str) -> List[dict] | List[tuple]:

        for attempt in range(1, MAX_RETRIES + 1):
            with self:
                print(f"Executing query atempt {attempt}... \n\n")
                try:
                    self.connection_session.execute(query)
                    rows = self.connection_session.fetchall()
                    return rows
                
                except Exception as err:
                    print(f"the following erro ocurred when reading table from snowflake : \n\n{err}\n\n")
                    print(f"Retrying in {RETRY_WAITING_TIME} seconds...")
                    time.sleep(RETRY_WAITING_TIME)

                    # handle with persistent query execution failure
                    if attempt == MAX_RETRIES:
                       raise Exception("Max Retries exceeded, may have some problem on snowflake connection.")

    def writeTableOnataform(self, query):
        with self:
            try:

                self.connection_session.execute(query)
                
            except Exception as err:
                print(f"the following erro ocurred when reading table from snowflake : \n\n{err}\n\n")

if __name__ == "__main__":
    query="""
            SELECT
                l_returnflag,
                l_linestatus,
                
            FROM
                lineitem
            LIMIT 30;    
    """
    snowflake_handler = SnowflakeDataPlataformHandler()
    print(snowflake_handler.readTableFromPlataform(query))
    