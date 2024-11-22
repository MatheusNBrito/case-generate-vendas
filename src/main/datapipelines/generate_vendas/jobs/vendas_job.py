from pyspark.sql import SparkSession
import configparser
from main.datapipelines.generate_vendas.commons.session.spark_session import SparkSessionWrapper  
from main.datapipelines.generate_vendas.books.variables import Variables
from main.datapipelines.generate_vendas.books.transformations import Transformations
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class VendaJob:
    def __init__(self):
        # Inicializa a sessão Spark através do wrapper
        self.spark_session_wrapper = SparkSessionWrapper()
        self.spark = self.spark_session_wrapper.spark
        print(self.spark)

    # Função para carregar os dados
    def load_data(self, raw_tables):
        vendas_raw_df = self.spark.read.parquet(raw_tables["VENDAS_PATH"]).select(*Variables.vendas_col_seq)
        pedidos_raw_df = self.spark.read.parquet(raw_tables["PEDIDOS_PATH"]).select(*Variables.pedidos_col_seq)
        itens_vendas_raw_df = self.spark.read.parquet(raw_tables["ITENS_VENDAS_PATH"]).select(*Variables.itens_vendas_col_seq)
        pedido_venda_raw_df = self.spark.read.parquet(raw_tables["PEDIDO_VENDA_PATH"]).select(*Variables.pedido_venda_col_seq)

        return vendas_raw_df, pedidos_raw_df, itens_vendas_raw_df, pedido_venda_raw_df
    
    # Função para salvar os dados
    def save_data(self, df, output_paths):
        df.write.mode("overwrite").parquet(output_paths)

        # Função principal
    def run_job(self):
        try:
            # Lê o arquivo de configuração
            conf = configparser.ConfigParser()
            conf.read('C:/Projetos/case-generate-vendas/src/main/datapipelines/generate_vendas/resources/application.conf')
            print("Seções carregadas:", conf.sections())
            print("VENDAS_PATH:", conf.get("input_paths", "raw_tables.VENDAS_PATH", fallback="Chave não encontrada"))
            # Acessa as configurações
            raw_tables = {
                "VENDAS_PATH": conf["input_paths"]["raw_tables.VENDAS_PATH"],
                "PEDIDOS_PATH": conf["input_paths"]["raw_tables.PEDIDOS_PATH"],
                "ITENS_VENDAS_PATH": conf["input_paths"]["raw_tables.ITENS_VENDAS_PATH"],
                "PEDIDO_VENDA_PATH": conf["input_paths"]["raw_tables.PEDIDO_VENDA_PATH"]
            }
            output_path_vendas = conf["output_paths"]["VENDAS_PATH"]
            print(f"VENDAS_PATH: {raw_tables['VENDAS_PATH']}")

            # Carrega os dados
            vendas_raw_df, pedidos_raw_df, itens_vendas_raw_df, pedido_venda_raw_df  = self.load_data(raw_tables)

            # Aplica as transformações
            final_vendas_df = Transformations.generate_vendas(vendas_raw_df, pedidos_raw_df, itens_vendas_raw_df,pedido_venda_raw_df)

            # Salva os dados
            self.save_data(final_vendas_df, output_path_vendas)

        except Exception as e:
            print(f"Erro durante a execução do job: {e}")
            raise  # Propaga o erro para análise

    def stop(self):
        # Fecha a sessão Spark
        if self.spark:
            self.spark.stop()


if __name__ == "__main__":
    job = VendaJob()
    try:
        job.run_job()
    finally:
        job.stop()