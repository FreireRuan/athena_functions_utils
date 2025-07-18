import io
import os
import json
import boto3
import time
import pandas as pd
import s3fs
from dotenv import load_dotenv


############# FUNÇÃO PARA INICIAR SESSÃO ATHENA E EXECUTAR CONSULTAS #############

def iniciar_sessao_athena(env_path=r"C:\Users\ruan.morais\Desktop\sandbox_freire\simulation-pipeline\credencial.env"):
    load_dotenv(env_path)
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    athena = session.client('athena')
    return athena, aws_access_key_id, aws_secret_access_key, aws_region

########## FUNÇÃO PARA EXECUTAR CONSULTAS NO ATHENA #########

def executar_query_athena(athena_client, query, database, output_location, aws_access_key_id, aws_secret_access_key, aws_region):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    query_execution_id = response['QueryExecutionId']

    # Espera a query finalizar
    state = 'RUNNING'
    while state in ['RUNNING', 'QUEUED']:
        result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        raise Exception(f"Query Athena falhou: {state}")

    s3_path = result['QueryExecution']['ResultConfiguration']['OutputLocation']

    # Lê o resultado direto do S3
    fs = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        client_kwargs={'region_name': aws_region}
    )
    with fs.open(s3_path, 'rb') as f:
        df = pd.read_csv(f)
    return df

########### FUNÇÃO PARA SALVAR DATAFRAME COMO PARQUET E JSON NO S3 #########

def save_parquet_s3(df, s3_path, fs):
    """
    Salva um DataFrame pandas como Parquet diretamente no S3.
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    with fs.open(s3_path, 'wb') as remote_file:
        remote_file.write(buffer.read())
    
def save_json_s3(obj, s3_path, fs):
    """
    Salva um objeto Python (dict/list) como JSON direto no S3.
    """
    buffer = io.BytesIO(json.dumps(obj, ensure_ascii=False, indent=2).encode('utf-8'))
    with fs.open(s3_path, 'wb') as remote_file:
        remote_file.write(buffer.read())

########## Inicialização do S3

def iniciar_fs_s3(aws_access_key_id, aws_secret_access_key, aws_region):
    """
    Retorna um S3FileSystem já autenticado, pronto para ler/escrever arquivos S3.
    """
    fs = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        client_kwargs={'region_name': aws_region}
    )
    return fs
