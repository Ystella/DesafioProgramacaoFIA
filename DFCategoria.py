from pyspark.sql.types import *
from pyspark.sql.functions import *
import time

df = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5'),
    ('id_cliente-3',  'cat-6, cat-7'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9'),
    ('id_cliente-9',  'cat-1'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10'),
    ('id_cliente-11', 'cat-1, cat-4, cat-7, cat-11, cat-2'),
    ('id_cliente-32', 'cat-25, cat-44, cat-1')
], ['id_cliente', 'categorias'])

df2 = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3, cat-15'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5, cat-11, cat-14'),
    ('id_cliente-3',  'cat-4, cat-14, cat-15'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10, cat-11'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10, cat-11, cat-13'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9, cat-12, cat-13, cat-14'),
    ('id_cliente-9',  'cat-2'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['id_cliente', 'categorias'])

def transforma(df_aux):
  # Seu script
  df_aux = df.select('id_cliente',
            split('id_cliente', '-')[1].cast(IntegerType()).alias('id_cli'),
            explode(split('categorias', ', ')).alias('cat'),
            split('cat', '-')[1].cast(IntegerType()).alias('cat_aux'),
           ).orderBy('cat_aux')

  df_aux = df_aux.groupBy('id_cli','id_cliente').pivot('cat_aux').count().orderBy('id_cli').drop('id_cli')

  for i in df_aux.columns:
    if i != 'id_cliente':
      df_aux = df_aux.withColumnRenamed(i, 'cat-'+i)

  df_aux.na.fill(0).show()
  
start = time.time()
transforma(df)
print(time.time() - start)

start = time.time()
transforma(df2)
print(time.time() - start)
