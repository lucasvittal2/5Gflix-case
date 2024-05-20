import boto3
import os

DATA_INTEGRATED_PATH="assets/data-integrated/"

class S3BucketHandler:
    """
    Descrição:
        Classe responsável por criar pastas e carrega arquvios .parquet no s3
        a respectiva pasta.
    """
    def __init__(self):
        """
        Descrição:
            Realização de conexão conexão com bucket s3 com credenciais de usuário.
        """  
        self.s3_client = boto3.client(
        service_name='s3',
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
    )
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")

    def uploadFolderToBucket(self, folder_path:str) -> None:
        """
        Argumentos:
            folder_path: caminho da pasta local a ser carregada no s3
        Descrição:
            Função que realiza criação de pasta no s3 e upload de arquivos locais dentro da pasta.

        """  

        folder_name = folder_path.split("/")[-1]
        print(f"created {folder_name} folder on s3. \n\n")
        self.s3_client.put_object(Bucket=self.bucket_name, Key=folder_name + "/")
        parquet_files =  [file for  file in os.listdir(folder_path) if file.endswith(".parquet")]

        for parquet_file in parquet_files:
            
            self.s3_client.upload_file(f"{folder_path}/{parquet_file}", self.bucket_name, folder_name + f"/{parquet_file}")    
            print(f"uploaded {folder_path}/{parquet_file}")



if __name__ == "__main__":
    """
    Descrição:
        implementação carregamento de dados das tabelas do DW no stagging do S3
    """
    
    s3_handler = S3BucketHandler()
    s3_handler.uploadFolderToBucket(DATA_INTEGRATED_PATH + "time_dimension")
    s3_handler.uploadFolderToBucket(DATA_INTEGRATED_PATH + "movie_dimension")
    s3_handler.uploadFolderToBucket(DATA_INTEGRATED_PATH + "fact_table")