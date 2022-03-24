# -*- coding: utf-8 -*-

#instalando o pymongo para conexão com o mongoDb
!pip install pymongo[srv]

#instalando o pyspark
!pip install pyspark

#instalando o google clous storage para autenticar o colab e fazer conexao
!pip install gcsfs

#immportações necessárias
import pymongo
from pymongo import MongoClient
import pandas as pd

#modulos para trabalhar com gcs
from google.cloud import storage
import os

#modulos para utilzar pyspark e pyspark.sql
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import SparkContext 
sc = SparkContext.getOrCreate() 

#montar drive via codigo; conexao do colab com o drive
from google.colab import drive
drive.mount('/content/drive')

# windows functions
from pyspark.sql.window import Window

#onde vou usar minhas credenciais para poder usar o cloudstorage
serviceAccount = '/content/projeto-individual-344413-8368fc2e6c37.json'

#faça minha credencial
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serviceAccount

#variaveis para conectar
client = storage.Client()     #intanciando o objeto

bucket = client.get_bucket('priscila-miranda-marketing')   #recebe o bucket que vou utilizar

bucket.blob('marketing_campaign.csv') #metodo blob para retornar nome do arquivo

path = 'gs://priscila-miranda-marketing/dadosBrutos/marketing_campaign.csv' #variavel path para colocar caminho

#conexão com o mongo cloud
client = pymongo.MongoClient("mongodb+srv://USER:PASSWORD@cluster0.xfgj7.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

#Onde eu digo o banco de dados e a coleçãp que quero acessar
db = client['projetoIndividual']
colecao = db.dadosBrutos

# Carrego meu arquivo da bucket para um dataframe em pandas
df_pandas = pd.read_csv(path, parse_dates=['Dt_Customer'],dayfirst=True)

df_pandas

# #Carregar data frame para mongo DB Atlas. A primeira linha é pra dropar o db para não duplicar os dados no MongoAtlas se rodar mais de uma vez
db.dadosBrutos.drop()
df_dici = df_pandas.to_dict("records")
#inserir colecao
colecao.insert_many(df_dici)

# Excluir colunas com informações iguais e que não entrarão nas análises principais
df_pandas.drop(columns=['Z_CostContact', 'Z_Revenue'], inplace = True)

# Verificando nomes das colunas do df
df_pandas.columns

# Ordenando colunas
df_pandas = df_pandas[['ID', 'Year_Birth', 'Education', 'Marital_Status', 'Income', 'Kidhome',
       'Teenhome', 'Dt_Customer', 'Recency', 'MntWines', 'MntFruits',
       'MntMeatProducts', 'MntFishProducts', 'MntSweetProducts',
       'MntGoldProds', 'NumDealsPurchases', 'NumWebPurchases',
       'NumCatalogPurchases', 'NumStorePurchases', 'NumWebVisitsMonth',
        'AcceptedCmp1', 'AcceptedCmp2', 'AcceptedCmp3', 'AcceptedCmp4', 'AcceptedCmp5', 'Complain', 'Response']]

# Renomeando colunas para o português-BR
df_pandas.rename({'Year_Birth':'Ano_nascimento', 'Education':'Educação', 'Marital_Status':'Status_civil', 'Income':'Renda_familiar', 'Kidhome':'Num_crianças',
       'Teenhome':'Num_adolescentes', 'Dt_Customer':'Data_cadastro', 'Recency':'Num_dias_ultima_compra', 'MntWines':'ValorGastoVinho', 'MntFruits':'ValorGastoFrutas',
       'MntMeatProducts':'ValorGastoCarne', 'MntFishProducts':'ValorGastoPeixe', 'MntSweetProducts':'ValorGastoDoces',
       'MntGoldProds':'ValorGastoOuro', 'NumDealsPurchases':'ComprasComDesconto', 'AcceptedCmp1':'AceiteOfertaPrimeiraCam',
       'AcceptedCmp2':'AceiteOfertaSegundaCam', 'AcceptedCmp3':'AceiteOfertaTerceiraCam', 'AcceptedCmp4':'AceiteOfertaQuartaCam', 'AcceptedCmp5':'AceiteOfertaQuintaCam', 'NumWebPurchases':'ComprasPeloSite',
       'NumCatalogPurchases':'ComprasPeloCatalogo', 'NumStorePurchases':'ComprasEmLoja', 'NumWebVisitsMonth':'VisitasSiteMes',
       'Complain':'Reclamação'}, axis=1, inplace=True)

df_pandas.head(2)

#Verificando valores únicos na coluna Educação
pd.unique(df_pandas['Educação'])

# Renomeando valores da coluna Educação para o português-BR
df_pandas['Educação'] = df_pandas['Educação'].replace(['Graduation', 'Master', 'Basic', '2n Cycle'], ['Graduacao', 'Mestrado', 'Basico', 'Pos-graduacao'])

#Verificando valores únicos na coluna Status_civil
pd.unique(df_pandas['Status_civil'])

# Renomeando valores da coluna Status_civil para o português-BR
df_pandas['Status_civil'] = df_pandas['Status_civil'].replace(['Single', 'Together', 'Married', 'Divorced', 'Widow', 'Alone'], ['Solteiro', 'Uniao_estavel', 'Casado', 'Divorciado', 'Viuvo', 'Sozinho'])

# Verificando quantidade e colunas com valores nulos no dataframe
df_pandas.isna().sum()

# Fazendo contagem de valores para cada coluna
df_pandas.count()

# Inicializando o spark session
spark = (
    SparkSession.builder
                .master('cloud')
                .appName('projetoIndividual')
                .config('spark.ui.port', '4050')
                .getOrCreate()
)

# Determinando um esquema com StructType (estrutura) e StructField. Definindo nome da coluna, tipo de dado da coluna e se pode ser nulo ou não.
schema = StructType([
                 StructField('ID', IntegerType(), True),
                 StructField('Ano_nascimento', StringType(), True),
                 StructField('Educacao', StringType(), True),
                 StructField('Status_civil', StringType(), True),
                 StructField('Renda_familiar', FloatType(), True),
                 StructField('Num_criancas', ShortType(), True),
                 StructField('Num_adolescentes', ShortType(), True),
                 StructField('Data_cadastro', DateType(), True),
                 StructField('Num_dias_ultima_compra', IntegerType(), True),
                 StructField('ValorGastoVinho', IntegerType(), True),
                 StructField('ValorGastoFrutas', IntegerType(), True),
                 StructField('ValorGastoCarne', IntegerType(), True),
                 StructField('ValorGastoPeixe', IntegerType(), True),
                 StructField('ValorGastoDoces', IntegerType(), True),
                 StructField('ValorGastoOuro', IntegerType(), True),
                 StructField('ComprasComDesconto', ShortType(), True),
                 StructField('ComprasPeloSite', ShortType(), True),
                 StructField('ComprasPeloCatalogo', ShortType(), True),
                 StructField('ComprasEmLoja', ShortType(), True),
                 StructField('VisitasSiteMes', ShortType(), True),
                 StructField('AceiteOferta1Cam', ShortType(), True),
                 StructField('AceiteOferta2Cam', ShortType(), True),
                 StructField('AceiteOferta3Cam', ShortType(), True),
                 StructField('AceiteOferta4Cam', ShortType(), True),
                 StructField('AceiteOferta5Cam', ShortType(), True),
                 StructField('Reclamacao', ShortType(), True),
                 StructField('Resposta', ShortType(), True)                  
]   
)

# Criando df no spark
df_sparkInicial = spark.createDataFrame(df_pandas, schema=schema)

df_sparkInicial.show(2)

df_spark = df_sparkInicial

# Filtrando os dados que possuem valores nulos em Renda_familiar para visualização e contagem
df_rendaNulos = df_spark.filter(df_spark["Renda_familiar"] == 'NaN')
df_rendaNulos.count()        # quantidade
df_rendaNulos.orderBy('Educacao', 'Ano_nascimento').show(50)         #visualização

# Filtragem dos que tem valores nulos em Renda_familiar de acordo com Status_civil para cálculo da porcentagem
qtdSolteiroRendaNulos = df_rendaNulos.filter(df_rendaNulos["Status_civil"] == 'Solteiro').count()
qtdCasadoRendaNulos = df_rendaNulos.filter(df_rendaNulos["Status_civil"] == 'Casado').count()
qtdUniaoEstavelRendaNulos = df_rendaNulos.filter(df_rendaNulos["Status_civil"] == 'Uniao_estavel').count()
qtdViuvoRendaNulos = df_rendaNulos.filter(df_rendaNulos["Status_civil"] == 'Viuvo').count()

# Aqui pode-se visualizar as variáveis do item anterior
qtdSolteiroRendaNulos
# qtdCasadoRendaNulos
# qtdUniaoEstavelRendaNulos
# qtdViuvoRendaNulos

# Filtragem dos dados totais de acordo com Status_civil para cálculo da porcentagem
qtdSolteiro = df_spark.filter(df_spark["Status_civil"] == 'Solteiro').count()
qtdCasado = df_spark.filter(df_spark["Status_civil"] == 'Casado').count()
qtdUniaoEstavel = df_spark.filter(df_spark["Status_civil"] == 'Uniao_estavel').count()
qtdViuvo = df_spark.filter(df_spark["Status_civil"] == 'Viuvo').count()

# Porcentagem de valores nulos de acordo com estado civil para verificar o impacto ao excluir
(qtdSolteiroRendaNulos * 100) / qtdSolteiro

# Porcentagem de valores nulos de acordo com estado civil para verificar o impacto ao excluir
(qtdCasadoRendaNulos * 100) / qtdCasado

# Porcentagem de valores nulos de acordo com estado civil para verificar o impacto ao excluir
(qtdUniaoEstavelRendaNulos * 100) / qtdUniaoEstavel

# Porcentagem de valores nulos de acordo com estado civil para verificar o impacto ao excluir
(qtdViuvoRendaNulos * 100) / qtdViuvo

# Considerando que as linhas com dados faltantes de renda correspondem à menos 2%, optou-se pela exclusão dessas linhas
df_spark = df_spark.na.drop(how='any')

# Visualizar quantidade total de linhas
linhas = df_spark.count()
linhas

# Visualizar quantidade total de linhas
col = len(df_spark.columns)
col

# Agrupar por Renda familiar e fazer a contagem
qtd_rendaFamiliar = df_spark.groupBy(F.col('Renda_familiar'))
qtd_rendaFamiliar.count().alias('Qtd').show(2)

qtd_educacao = df_spark.groupBy(F.col('Educacao'))
qtd_educacao.count().sort(F.col('count')).show()

'''
A maior quantidade de clientes possui graduação
'''

# Quantidade de pessoas de acordo com Status_civil
qtd_estadocivil = df_spark.groupBy(F.col('Status_civil'))
qtd_estadocivil.count().sort(F.col('count')).show()

'''
A maior quantidade de clientes é casado. seguido por união estável
'''

# Devido YOLO, Absurd e Sozinho representarem aproximadamente 0,315% apenas do total, foram renomeados para Outros
df_spark = df_spark.withColumn('Status_civil', 
                          F.when(F.col('Status_civil') == 'Solteiro', F.lit('Solteiro'))
                           .when(F.col('Status_civil') == 'Uniao_estavel', F.lit('Uniao_estavel'))
                           .when(F.col('Status_civil') == 'Casado', F.lit('Casado'))
                           .when(F.col('Status_civil') == 'Divorciado', F.lit('Divorciado'))
                           .when(F.col('Status_civil') == 'Viuvo', F.lit('Viuvo'))
                           .otherwise(F.lit(('Outros')))
)

# Quantidade de pessoas de acordo com Status_civil após renomear YOLO, Absurd e Sozinho como Outros
qtd_estadocivil = df_spark.groupBy(F.col('Status_civil'))
qtd_estadocivil.count().sort(F.col('count')).show()

# Filtragem para avaliar se os que possuem Status Civil = Outros possam ser as mesmas pessoas.
df_outros = df_spark.filter(df_spark["Status_civil"] == 'Outros')
df_outros.show()

'''
Pode-se perceber que provavelmente os 2 últimos clientes é a mesma pessoa, pois os dados são todos iguais, com a única exceção da coluna Resposta
'''

# Soma das campanhas aceitas em uma única coluna para saber a quantidade de campanhas aceitas por cliente
df_spark = df_spark.withColumn(
    'CampanhasAceitas', df_spark['AceiteOferta1Cam'] + df_spark['AceiteOferta2Cam'] + df_spark['AceiteOferta3Cam']
    + df_spark['AceiteOferta4Cam'] + df_spark['AceiteOferta5Cam']
)

# Exclusão das colunas de aceites por campanha
df_spark = df_spark.drop('AceiteOferta1Cam','AceiteOferta2Cam','AceiteOferta3Cam','AceiteOferta4Cam','AceiteOferta5Cam')

# Soma do valor gasto em uma única coluna para saber o total gasto por cliente
df_spark = df_spark.withColumn(
    'GastoTotal', df_spark['ValorGastoVinho'] + df_spark['ValorGastoFrutas'] + df_spark['ValorGastoCarne']
    + df_spark['ValorGastoPeixe'] + df_spark['ValorGastoDoces'] + df_spark['ValorGastoOuro']
)

df_spark.show(2)

# Quantidade de pessoas por ano de nascimento
qtd_nascimento = df_spark.groupBy(F.col('Ano_nascimento'))
qtd_nascimento.count().sort(F.col('Ano_nascimento')).show()

# Criando coluna com faixa etária para posterior agrupamento
df_spark = df_spark.withColumn('FaixadeIdade', 
                          F.when((F.col('Ano_nascimento') > 1890) & (F.col('Ano_nascimento') < 1921),    F.lit('Mais de 100 anos'))
                           .when((F.col('Ano_nascimento') >= 1921) & (F.col('Ano_nascimento') < 1941),  F.lit('Entre 80 e 100 anos'))
                           .when((F.col('Ano_nascimento') >= 1941) & (F.col('Ano_nascimento') < 1961),  F.lit('Entre 60 e 80 anos'))
                           .when((F.col('Ano_nascimento') >= 1961) & (F.col('Ano_nascimento') < 1981),  F.lit('Entre 40 e 60 anos'))
                           .when((F.col('Ano_nascimento') >= 1981) & (F.col('Ano_nascimento') <= 1996), F.lit('Entre 20 e 40 anos'))
)

# Agrupando de acordo com a nova coluna criada 'FaixadeIdade' para visualização da faixa etária de clientes
faixaEtaria = df_spark.groupBy(F.col('FaixadeIdade'))
faixaEtaria.count().sort(F.col('FaixadeIdade')).show()

'''
A faixa etária predominante de clientes está entre 40 e 80 anos e corresponde cerca de 80%
'''

# Criando coluna com faixa de valores para posterior agrupamento
df_spark = df_spark.withColumn('FaixaSalarial', 
                          F.when((F.col('Renda_familiar') > 1000) & (F.col('Renda_familiar') < 20000),    F.lit('1000----|20000'))
                           .when((F.col('Renda_familiar') >= 20000) & (F.col('Renda_familiar') < 40000),  F.lit('20000---|40000'))
                           .when((F.col('Renda_familiar') >= 40000) & (F.col('Renda_familiar') < 60000),  F.lit('40000---|60000'))
                           .when((F.col('Renda_familiar') >= 60000) & (F.col('Renda_familiar') < 80000),  F.lit('60000---|80000'))
                           .when((F.col('Renda_familiar') >= 80000) & (F.col('Renda_familiar') < 100000), F.lit('80000--|100000'))
                           .otherwise(F.lit(('Acima de 100000')))
)

# Agrupando de acordo com a nova coluna criada 'FaixaSalarial' para visualização da faixa salarial predominante dos clientes
faixaSalarial = df_spark.groupBy(F.col('FaixaSalarial'))
faixaSalarial.count().sort(F.col('FaixaSalarial')).show()

'''
A faixa salarial dos clientes está em sua maioria entre 20000 e 80000
'''

# Quantidade de cadastros por data
qtd_cadastro = df_spark.groupBy(F.col('Data_cadastro'))
qtd_cadastro.count().sort(F.col('Data_cadastro')).show()

# Criando coluna com período de cadastros para posterior agrupamento e melhor visualização dos dados
df_spark = df_spark.withColumn('PeriodoCadastro', 
                          F.when((F.year('Data_cadastro') == 2012) & ((F.month(F.col('Data_cadastro')) >=1) & (F.month(F.col('Data_cadastro')) <=3)),    F.lit('1º trimestre 2012'))
                           .when((F.year('Data_cadastro') == 2012) & ((F.month(F.col('Data_cadastro')) >=4) & (F.month(F.col('Data_cadastro')) <=6)),  F.lit('2º trimestre 2012'))
                           .when((F.year('Data_cadastro') == 2012) & ((F.month(F.col('Data_cadastro')) >=7) & (F.month(F.col('Data_cadastro')) <=9)),  F.lit('3º trimestre 2012'))
                           .when((F.year('Data_cadastro') == 2012) & ((F.month(F.col('Data_cadastro')) >=10) & (F.month(F.col('Data_cadastro')) <=12)),  F.lit('4º trimestre 2012'))
                           .when((F.year('Data_cadastro') == 2013) & ((F.month(F.col('Data_cadastro')) >=1) & (F.month(F.col('Data_cadastro')) <=3)), F.lit('1º trimestre 2013'))
                           .when((F.year('Data_cadastro') == 2013) & ((F.month(F.col('Data_cadastro')) >=4) & (F.month(F.col('Data_cadastro')) <=6)), F.lit('2º trimestre 2013'))
                           .when((F.year('Data_cadastro') == 2013) & ((F.month(F.col('Data_cadastro')) >=7) & (F.month(F.col('Data_cadastro')) <=9)), F.lit('3º trimestre 2013'))
                           .when((F.year('Data_cadastro') == 2013) & ((F.month(F.col('Data_cadastro')) >=10) & (F.month(F.col('Data_cadastro')) <=12)), F.lit('4º trimestre 2013'))
                           .when((F.year('Data_cadastro') == 2014) & ((F.month(F.col('Data_cadastro')) >=1) & (F.month(F.col('Data_cadastro')) <=3)), F.lit('1º trimestre 2014'))
                           .when((F.year('Data_cadastro') == 2014) & ((F.month(F.col('Data_cadastro')) >=4) & (F.month(F.col('Data_cadastro')) <=6)), F.lit('2º trimestre 2014'))
                           .when((F.year('Data_cadastro') == 2014) & ((F.month(F.col('Data_cadastro')) >=7) & (F.month(F.col('Data_cadastro')) <=9)), F.lit('3º trimestre 2014'))
                           .when((F.year('Data_cadastro') == 2014) & ((F.month(F.col('Data_cadastro')) >=10) & (F.month(F.col('Data_cadastro')) <=12)), F.lit('4º trimestre 2014'))
)

# Agrupando de acordo com a nova coluna criada 'PeriodoCadastro' para visualização da quantidade de cadastros por trimestre
cadastros = df_spark.groupBy(F.col('PeriodoCadastro'))
cadastros.count().sort(F.col('PeriodoCadastro')).show()

'''
Os registros iniciaram no 3º trimestre de 2012 e finalizaram no 2º trimestre de 2014 totalizando 2 anos de registros.
Durante o período observa-se certa constância no nº de cadastros de clientes
'''

# Renomeando algumas colunas com spark
df_spark = (df_spark.withColumnRenamed('Status_civil', 'EstadoCivil')
              .withColumnRenamed('Renda_familiar', 'RendaFamiliar')
              .withColumnRenamed('Ano_nascimento', 'AnoNascimento'))

# Selecionando valores distintos na coluna
df_campanhasAceitas= df_spark.select(F.col('CampanhasAceitas')).distinct().sort(F.col("CampanhasAceitas")).show()

# Campanhas aceitas de acordo com o Estado Civil e Grau de escolaridade
df_spark.groupBy(F.col("EstadoCivil"), F.col("Educacao")).sum("CampanhasAceitas").sort(F.col('EstadoCivil'), F.col('Educacao')).show(2240)

'''
Os clientes casados são os que possuem a maior quantidade de campanhas aceitas independente do grau de instrução
'''

df_spark.show(2)

# Qtd decrianças/adolescentes de acordo com o Ano de nascimento e Grau de escolaridade
df_spark.groupBy(F.col("AnoNascimento"), F.col("Educacao")).sum("Num_criancas", "Num_adolescentes").sort(F.col('AnoNascimento'), F.col('Educacao')).show()
'''
Os clientes que possuem o maior nº de crianças ou adolescentes em casa, são os que possuem Graduação
'''

# Compras pelo site de acordo com o EstadoCivil
df_spark.groupBy(F.col("EstadoCivil")).agg(F.sum("ComprasPeloSite").alias('QtdCompras')).show()

# Compras pelo catalogo de acordo com o EstadoCivil
df_spark.groupBy(F.col("EstadoCivil")).agg(F.sum("ComprasPeloCatalogo").alias('QtdCompras')).show()

# Compras na loja de acordo com o EstadoCivil
df_spark.groupBy(F.col("EstadoCivil")).agg(F.sum("ComprasEmLoja").alias('QtdCompras')).show()

# Visitas ao site no ultimo mes
df_spark.groupBy(F.col("EstadoCivil")).agg(F.sum("VisitasSiteMes").alias('QtdVisitas')).show()

# Gasto com vinho de acordo com escolaridade
( df_spark.groupBy(F.col("Educacao"))
           .agg(F.sum("GastoTotal"),
                F.sum("ValorGastoVinho"),
                F.max("ValorGastoVinho"),
                F.min("ValorGastoVinho"),
                F.avg("ValorGastoVinho")).show() 
)

# Gasto com frutas de acordo com escolaridade
( df_spark.groupBy(F.col("Educacao"))
           .agg(F.sum("GastoTotal"),
                F.sum("ValorGastoFrutas"),
                F.max("ValorGastoFrutas"),
                F.min("ValorGastoFrutas"),
                F.avg("ValorGastoFrutas")).show() 
)

# Gasto com carne de acordo com escolaridade
( df_spark.groupBy(F.col("Educacao"))
           .agg(F.sum("GastoTotal"),
                F.sum("ValorGastoCarne"),
                F.max("ValorGastoCarne"),
                F.min("ValorGastoCarne"),
                F.avg("ValorGastoCarne")).show() 
)

# Gasto com peixe de acordo com escolaridade
( df_spark.groupBy(F.col("Educacao"))
           .agg(F.sum("GastoTotal"),
                F.sum("ValorGastoPeixe"),
                F.max("ValorGastoPeixe"),
                F.min("ValorGastoPeixe"),
                F.avg("ValorGastoPeixe")).show() 
)

# Gasto com doces de acordo com escolaridade
( df_spark.groupBy(F.col("Educacao"))
           .agg(F.sum("GastoTotal"),
                F.sum("ValorGastoDoces"),
                F.max("ValorGastoDoces"),
                F.min("ValorGastoDoces"),
                F.avg("ValorGastoDoces")).show() 
)

# Gasto com produtos premium? de acordo com escolaridade
( df_spark.groupBy(F.col("Educacao"))
           .agg(F.sum("GastoTotal"),
                F.sum("ValorGastoOuro"),
                F.max("ValorGastoOuro"),
                F.min("ValorGastoOuro"),
                F.avg("ValorGastoOuro")).show() 
)

# Compras com desconto de acordo com escolaridade
( df_spark.groupBy(F.col("Educacao"))
           .agg(F.sum("GastoTotal"),
                F.sum("ComprasComDesconto"),
                F.max("ComprasComDesconto"),
                F.min("ComprasComDesconto"),
                F.avg("ComprasComDesconto")).show() 
)

df_spark.show(2)

# Particionando e ordenando por faixa etária
w0 = Window.partitionBy(F.col('FaixadeIdade')).orderBy('FaixadeIdade')

df_spark.withColumn('NLinhas', F.row_number().over(w0)).show(2)

df_spark.withColumn('Ranking', F.dense_rank().over(w0)).show()

df_spark_final = df_spark

# Convertendo df final para pandas para load
df_final = df_spark_final.toPandas()

# Verificando que a coluna Data_cadastro está como datetype
df_spark_final.schema

# Passando a coluna Data_cadastro para string pois o mongo não aceita o tipo datetime.date
tipoData_dict = {'Data_cadastro': str}
df_final = df_final.astype(tipoData_dict)

# Subindo df tratado para GCP
client = storage.Client()
bucket = client.get_bucket('priscila-miranda-marketing')
bucket.blob('dadoTratados/dadosTratados.csv').upload_from_string(df_final.to_csv(), 'text/csv')

# Subindo df tratado para coleção separada dos dados brutos no Mongo Atlas

#conexão com o mongo cloud
client = pymongo.MongoClient("mongodb+srv://USER:PASSWORD@cluster0.xfgj7.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

db = client['projetoIndividual']
colecaoF = db.dadosTratados

db.dadosTratados.drop()
df_diciF = df_final.to_dict("records")
#inserir colecao
colecaoF.insert_many(df_diciF)

# Consultas com spark.sql

# Aqui utilizo o dataframe final em spark
df_spark_final.show(5)

df_spark_final.createOrReplaceTempView("marketing")

"""### Quantidade de cadastros por ano de registros"""

spark.sql('select * from marketing where year(Data_cadastro) == 2012').count()

spark.sql('select * from marketing where year(Data_cadastro) == 2013').count()

spark.sql('select * from marketing where year(Data_cadastro) == 2014').count()

"""### Número máximo de compras na loja por cliente, para saber quais clientes fizeram mais comprasna loja de acordo com grau de instrução"""

spark.sql('select max(ComprasEmLoja) from marketing').show()

spark.sql('select * from marketing where (ComprasEmLoja == 13) and (Educacao == "Graduacao")').count()

# Clientes que possuem graduação ou PhD foram os que mais compraram na loja física

spark.sql('select * from marketing where (ComprasEmLoja == 13) and (Educacao == "Mestrado")').count()

spark.sql('select * from marketing where (ComprasEmLoja == 13) and (Educacao == "PhD")').count()

spark.sql('select * from marketing where (ComprasEmLoja == 13) and (Educacao == "Pos-graduacao")').count()

"""Número máximo de compras pelo site por cliente, para saber quais clientes fizeram mais compras pelo site de acordo com grau de instrução"""

spark.sql('select max(ComprasPeloSite) from marketing').show()

spark.sql('select * from marketing where (ComprasPeloSite > 10) order by Educacao').show(90)

spark.sql('select * from marketing where (ComprasPeloSite > 10) and \
            ((FaixadeIdade == "Entre 40 e 60 anos") or (FaixadeIdade == "Entre 60 e 80 anos"))').count()


#Interessante observar que os clientes que fizeram mais de 10 compras pelo site possuem mais de 40 anos
#E apenas 1 cliente fez 27 compras pelo site no período

"""
1) O maior número de clientes possuem graduação.
2) O maior número de clientes são casados, seguido por união estável. Maior parte possui relacionamento estável.
3) Os clientes que possuem graduação são os que tem o maior número de crianças ou adolescentes.
4) Talvez devido o item anterior, o gasto total de compras seja maior com estes clientes.
5) A faixa etária dos consumidores é de 40 a 60 anos.
6) A maior quantidade de campanhas aceitas foram pelos clientes casados (seguido por união estpavel) independente do grau acadêmico. 
"""