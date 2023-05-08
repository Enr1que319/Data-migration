import pandas as pd
from pyspark.sql import SparkSession
import sqlalchemy

spark = SparkSession.builder.master('local').appName('Write Avro').getOrCreate()
dbEngine = sqlalchemy.create_engine("postgresql+psycopg2://postgres:stdio.h@localhost:5432/test")

####################################################################################################

def exportData_Avro():
    """
    :return: returns None.
    """

    tables = ['departments','jobs','hired_employees']
    path = '/Users/enriquevazquez/Desktop/Data-migration/Avro_Export'

    try:
        for tbl in tables:
            df = pd.read_sql(f'SELECT * FROM {tbl}', dbEngine)
            spkDf = spark.createDataFrame(df)
            spkDf.write.format('avro').mode('overwrite').save(f'{path}/{tbl}/{tbl}-export')
    except Exception as e:
        print (e, flush = True)
        return None

