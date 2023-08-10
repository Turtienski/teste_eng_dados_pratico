from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import logging
import boto3
import time
import json
import io

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage(),
            'logger_name': record.name,
            'module': record.module,
            'file_path': record.pathname,
            'line_number': record.lineno,
            'function_name': record.funcName
        }
        return json.dumps(log_data, ensure_ascii=False)

class DataProcessor:
    def __init__(self, bucket_name, folder_prefix, db_url, db_table_name, db_engine):
        self.bucket_name = bucket_name
        self.folder_prefix = folder_prefix
        self.db_url = db_url
        self.db_table_name = db_table_name
        self.db_engine = db_engine
        self.s3 = boto3.client('s3')
        self.cloudwatch = boto3.client('cloudwatch')
        self.df = ''

        # Configurar o logger
        logging.basicConfig(level=logging.INFO)

        # Cria manipulador JSON
        stdout_handler = logging.StreamHandler()
        json_formatter = JsonFormatter()
        stdout_handler.setFormatter(json_formatter)

        # Inicia Logger                
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(stdout_handler)


    def get_df(self, path):
        self.logger.debug('Iniciando Leitura do CSV')
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=path)
        self.df = pd.read_csv(io.BytesIO(obj['Body'].read()),
                         on_bad_lines='warn',
                         sep=','
                        )
        self.logger.info('Dataframe Carregado')


    def transform_clear(self):
        self.logger.debug('Limpando duplicados')
        self.df = self.df.drop_duplicates()
        self.logger.debug('Removendo valores vazios')
        self.df = self.df.fillna(0)
        self.logger.info('Limpeza finalizada')
        
        self.add_insert_date()

    def add_insert_date(self): # desafio 5
        # Obter a data atual
        current_date = datetime.now().date()
        # Adicionar a data atual como uma nova coluna no DataFrame
        self.df['insert_date'] = current_date


    def transform_convert_currency(self, data, cur='USD', convRate=0.75):
        self.logger.debug('Convertendo valor monetario')
        self.df['sale_value'] = self.df['sale_value'].apply(lambda x: x*convRate)
        self.logger.debug('Alterando moeda')
        self.df['currency'] = cur
        self.logger.info('Valores alterados')
    
    def data_quality(self):
        # Verificar duplicatas nos IDs de usuário
        duplicates = self.df.duplicated(subset=['user_id'])
        if duplicates.any():
            self.logger.warn("IDs de usuários duplicados encontrados")


        # Verificar valores de vendas negativos
        negative_sales = self.df[self.df['sale_value'] < 0]
        if not negative_sales.empty:
            self.logger.warn("Valores de vendas negativos encontrados")


        # Garanta que todas as entradas tenham timestamps válidos.
        self.df['date'] = pd.to_datetime(self.df['date'], errors='coerce')
        invalid_timestamps = self.df[self.df['timestamp'].isnull()]
        if not invalid_timestamps.empty:
            self.logger.warn("Entradas com timestamps inválidos encontradas")

        # Quantidade de linhas ingeridas no banco de dados de sua escolha é igual a quantidade de linhas originais
        #To Do

    def process_and_load_data(self):
        start_time = time.time() # desafio 5

        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.folder_prefix)
        self.logger.info('Obtendo lista de objetos')
        if 'Contents' in response:
            with self.db_engine.connect() as connection:
                for obj in response['Contents']:
                    self.logger.info('Processando objeto: ' + obj['Key'])
                    self.get_df(obj['Key'])
                    self.data_quality() # desafio 5
                    self.transform_clear()
                    self.transform_convert_currency()
                    
                    self.df.to_sql(self.db_table_name, connection, index=False, if_exists='append')
                    self.logger.info('Escrita finalizada com sucesso')
    
        else:
            self.logger.info('Nenhum objeto encontrado na pasta.')
        # desafio 5    
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        self.cloudwatch.put_metric_data(
            Namespace='ETLProcess',
            MetricData=[{
                'MetricName': 'ETLExecutionTime',
                'Value': elapsed_time,
                'Unit': 'Seconds'
            }]
        )

def main():
    try:
        logging.basicConfig(format='%(name)s - %(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
        # Valores podem vir de um parameter store, argumentos e secret manager
        bucket_name = 'julio-datalake'
        folder_prefix = 'transient-zone/sales/sales_data/'
        db_user = 'SEU_USUARIO'
        db_password = 'SUA_SENHA'
        db_host = 'ENDPOINT_DO_RDS_AURORA'
        db_port = '5432'
        db_name = 'NOME_DO_BANCO_DE_DADOS'
        table_name = 'sales_data'
        db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        
        db_engine = create_engine(db_url)
        
        processor = DataProcessor(bucket_name, folder_prefix, db_url, table_name, db_engine)
        processor.process_and_load_data()
    
    except Exception as e: #desafio 5
        # Registrar detalhes do erro no log
        logging.error(f'[ETL-vendas] Erro no pipeline ETL: {str(e)}')
        raise

if __name__ == "__main__":
    main()
