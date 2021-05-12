import os
os.environ['HAIL_DIR'] = '/home/cdsw/.local/lib/python3.6/site-packages/hail/backend'

from pyspark.sql import SparkSession

storage = os.environ['STORAGE']
hail_dir = os.environ['HAIL_DIR']
hail_data = os.environ['HAIL_DATA']

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.executor.memory","4g")\
    .config("spark.executor.cores","2")\
    .config("spark.driver.memory","2g")\
    .config("spark.executor.instances","4")\
    .config("spark.yarn.access.hadoopFileSystems",storage)\
    .config("spark.jars",hail_dir + "/hail-all-spark.jar")\
    .config("spark.driver.extraClassPath",hail_dir + "/hail-all-spark.jar")\
    .config("spark.executor.extraClassPath",hail_dir + "/hail-all-spark.jar")\
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
    .config("spark.kryo.registrator","is.hail.kryo.HailKryoRegistrator")\
    .getOrCreate()

import hail as hl
     
hl.init(sc=spark.sparkContext,tmp_dir = hail_data + "tmp")

from hail.plot import show
from pprint import pprint
hl.plot.output_notebook()

hl.utils.get_1kg(hail_data)
hl.import_vcf(hail_data + "/1kg.vcf.bgz").write(hail_data + "/1kg.mt", overwrite=True)

mt = hl.read_matrix_table(hail_data + "/1kg.mt")
mt.rows().select().show(5)
mt.row_key.show(5)
mt.s.show(5)
mt.entry.take(5)

table = (hl.import_table(hail_data + "/1kg_annotations.txt", impute=True)
         .key_by('Sample'))

table.describe()
table.show(width=100)

print(mt.col.dtype)

mt = mt.annotate_cols(pheno = table[mt.s])
mt.col.describe()

#hl.stop()

