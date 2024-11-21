from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DecimalType, LongType, IntegerType
from main.datapipelines.generate_vendas.books import constants
from main.datapipelines.generate_vendas.books.functions import Functions  
from pyspark.sql import SparkSession

# Inicialização do SparkSession
spark = SparkSession.builder.appName("VariablesVendas").getOrCreate()
    
class Variables:
    # Referência para funções auxiliares
    functions = Functions()

    # vendasColSeq - colunas da tabela "vendas_raw"
    vendasColSeq = [
        F.col(constants.D_DT_VD).cast(DateType()).alias(constants.DATA_EMISSAO),
        F.col(constants.N_ID_FIL).cast(LongType()).alias(constants.CODIGO_FILIAL),
        F.col(constants.N_ID_VD_FIL).cast(LongType()).alias(constants.CODIGO_CUPOM_VENDA),
        F.col(constants.V_CLI_COD).cast(StringType()).alias(constants.CODIGO_CLIENTE)
    ]

    # itensVendasColSeq - colunas da tabela "itens_vendas_raw"
    itensVendasColSeq = [
        F.col(constants.N_ID_FIL).cast(LongType()).alias(constants.CODIGO_FILIAL),
        F.col(constants.N_ID_VD_FIL).cast(LongType()).alias(constants.CODIGO_CUPOM_VENDA),
        F.col(constants.N_ID_IT).cast(LongType()).alias(constants.CODIGO_ITEM),
        F.col(constants.V_IT_VD_CONV).cast(StringType()).alias(constants.V_IT_VD_CONV),  
        F.col(constants.N_VLR_VD).cast(DecimalType(18, 2)).alias(constants.N_VLR_VD),  
        F.col(constants.N_VLR_DESC).cast(DecimalType(18, 2)).alias(constants.N_VLR_DESC),  
        F.col(constants.N_QTD).cast(IntegerType()).alias(constants.QUANTIDADE)
    ]

    pedidoVendaColSeq = [
        F.col(constants.N_ID_FIL).cast(LongType()).alias(constants.CODIGO_FILIAL),
        F.col(constants.N_ID_VD_FIL).cast(LongType()).alias(constants.CODIGO_CUPOM_VENDA),
        F.col(constants.N_ID_PDD).cast(LongType()).alias(constants.N_ID_PDD)
    ]

    pedidosColSeq = [
        F.col(constants.N_ID_PDD).cast(LongType()).alias(constants.N_ID_PDD),
        F.col(constants.V_CNL_ORIG_PDD).cast(StringType()).alias(constants.CANAL_VENDA)
    ]

    itensVendasSelCol = [
        F.col(constants.CODIGO_FILIAL),  # Removido o alias redundante
        F.col(constants.CODIGO_CUPOM_VENDA),
        F.col(constants.CODIGO_ITEM),
        F.col(constants.QUANTIDADE),
        F.col(constants.TIPO_DESCONTO),
        F.col(constants.VALOR_UNITARIO)
    ]
