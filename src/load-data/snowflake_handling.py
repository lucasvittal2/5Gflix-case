import os
import snowflake.connector
from typing import List



class DataPlaformHandler:
    """
        Descrição:
            Classe abstrata para implementar classes que façam a interface com plataformas de SGBDS.
            Esta classe visa declarar que este modelo esta aberto para extenção para outras classes
            que herdaram os atributos desta calsse.

            OUa função dessa classe é implementar a inversão de dependencia em implamentações com este módulo.
    """

    def runQuery(self, query: str) -> List[dict] | List[tuple]:
        """
            Argumentos:
                query: query a ser executada pelo SGBD
            
            Descrição:
                método que implementa envio de query para sgbd e retorna sua resposta
            
            Saída:
                Saída com linhas da tabela ou lista de documentos
        """
        raise NotImplementedError("The method 'readTableFromPlataform' were inhereted but not implemeted !")
    

    
class SnowflakeDataPlataformHandler(DataPlaformHandler):
    """
        Classe que faz o interfaceamento com Snowflake pemitindo execução de queries via script python.
        Além disso esta classe realiza controle de contexto, afim de que o usuário só se preocupe em usar a 
        classe para executar suas queries no snowflake.
    """

    def __init__(self) -> None:
        """
            Descrição:
                Faz  conexão com  o snowflake a partir das credenciais de usuário e apontamento
                de banco de dados.
        """
        self.connection = snowflake.connector.connect(
                                                user=os.getenv("USER_NAME"),
                                                password=os.getenv("USER_PASSWORD"),
                                                account=os.getenv("USER_ACCOUNT"),
                                                database=os.getenv("DATABASE_NAME")                                      )

    def __del__(self):
        """
            Descrição:
                Controle de contexto que  desconecta do snowflake semre que o programa é terminado de executar.
        """
  
        self.connection.close()


    def __enter__(self):
        """
            Descrição:
                Controle de contexto que  conecta a uma sessão de conexão do snowflake sempre que a execução de 
                uma query é inicializada de ser executada
        """       
        self.connection_session = self.connection.cursor()


    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        """
        Descrição:
            Controle de contexto que  desconecta da sessão de conexão do snowflake sempre que a execução de 
            uma query é finalizada
        """   
  
        self.connection_session.close()

        

    def runQuery(self, query: str) -> List[dict] | List[tuple]:
            """
            Argumentos:
                query: Query a ser execurtada pelo SGBD do Snowflake.

            Descrição:
                Função que implementa execução de query com controle contexto, conectando e 
                desconectando da seção automaticamente, esta função faz o tratamento de exeção
                caso haja algum erro na solitação da requisição poista depende de fatores externos ao programa
                tal como conexãoa internet ou servidor do snowflake estar disponivel, ou usuário estar 
                credenciado ou habilitado para uso.
            Saída:
                Lista com tuplas de linhas retornada pela consulta ou None caso a consulta não retorn
                nenhum valor.
            
            """  
            with self:
                try:
                    self.connection_session.execute(query)
                    rows = self.connection_session.fetchall()
                    return rows
                
                except Exception as err:
                    
                    raise Exception("Failure on execute query due to following error: \n\n{err}\n\n")
                    
                    

                    
    