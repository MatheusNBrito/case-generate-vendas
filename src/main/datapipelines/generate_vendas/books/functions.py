from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when,trim
from main.datapipelines.generate_vendas.books import constants
from typing import List 
from pyspark.sql.types import DecimalType
from pyspark.sql import functions as F

class Functions:

    def create_canal_venda_col(df: DataFrame) -> DataFrame:
        return df.withColumn(
        "CANAL_VENDA",  # Nome da coluna a ser criada/atualizada
            when(col("CANAL_VENDA") == "L", "Loja")
            .when(col("CANAL_VENDA") == "S", "Site")
            .when(col("CANAL_VENDA") == "A", "App")
            .otherwise(None)
    )

    def create_tipo_desconto_col(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "TIPO_DESCONTO",  
            when((col("N_VLR_DESC") > 0) & (trim(col("V_IT_VD_CONV")) == "SIM"), "Convênio")
            .otherwise(
                when(col("N_VLR_DESC") > 0, "Promoção")
                .otherwise(None)
            )
    )

    def create_valor_unitario(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "VALOR_UNITARIO",  
            (col("N_VLR_VD") / col("QUANTIDADE")).cast(DecimalType(18, 2))
    )

    def treat_null(df: DataFrame) -> DataFrame:
        return df.na.fill("-1").na.fill(-1)