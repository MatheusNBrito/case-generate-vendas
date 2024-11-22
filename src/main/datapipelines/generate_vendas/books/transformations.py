from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from main.datapipelines.generate_vendas.books.functions import Functions
from main.datapipelines.generate_vendas.books.variables import Variables
from main.datapipelines.generate_vendas.books import constants


class Transformations:
    def generate_vendas(vendas_raw_df: DataFrame,
                        pedidos_raw_df: DataFrame,
                        itens_vendas_raw_df: DataFrame,
                        pedido_venda_raw_df: DataFrame) -> DataFrame:
    # Transformações no DataFrame vendas_raw_df
        vendas_transformed_df = (
            vendas_raw_df
            .filter(F.col("CODIGO_FILIAL").isNotNull() & F.col("CODIGO_CUPOM_VENDA").isNotNull())
            .dropDuplicates()
        )

        # Transformações no DataFrame pedidos_raw_df
        pedidos_transformed_df = (
            pedidos_raw_df
            .join(pedido_venda_raw_df, on=["N_ID_PDD"], how="inner")
            .filter(F.col("CODIGO_FILIAL").isNotNull() & F.col("CODIGO_CUPOM_VENDA").isNotNull())
            .transform(Functions.create_canal_venda_col)  # Chama a função para criar a coluna CANAL_VENDA
            .select("CODIGO_FILIAL", "CODIGO_CUPOM_VENDA", "CANAL_VENDA")
        )

        # Transformações no DataFrame itens_vendas_raw_df
        itens_vendas_transformed_df = (
            itens_vendas_raw_df
            .filter(F.col("CODIGO_FILIAL").isNotNull() & F.col("CODIGO_CUPOM_VENDA").isNotNull())
            .dropDuplicates()
            .transform(Functions.create_tipo_desconto_col)  # Cria coluna TIPO_DESCONTO
            .transform(Functions.create_valor_unitario)  # Cria coluna VALOR_UNITARIO
            .select(Variables.itens_vendas_sel_col)  # Seleciona colunas finais para itens de vendas
        )

        # Combinação final dos DataFrames
        vendas_df = (
            itens_vendas_transformed_df
            .join(vendas_transformed_df, on=["CODIGO_FILIAL", "CODIGO_CUPOM_VENDA"], how="inner")
            .join(pedidos_transformed_df, on=["CODIGO_FILIAL", "CODIGO_CUPOM_VENDA"], how="inner")
            .transform(Functions.treat_null)  # Trata valores nulos
        )

        return vendas_df