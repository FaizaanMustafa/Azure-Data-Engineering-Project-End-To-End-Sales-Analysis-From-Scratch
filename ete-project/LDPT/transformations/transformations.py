from pyspark import pipelines as dp
from pyspark.sql.functions import *

my_rules = {'rule1':'product_id IS NOT NULL',
            'rule2':'product_name IS NOT NULL'}

@dp.table
@dp.expect_all_or_drop(my_rules)
def DimProducts_stage():
  df = spark.readStream.table('ete_project.silver.products')
  return df

@dp.view
def DimProducts_view():
  df = spark.readStream.table('live.DimProducts_stage')
  return df

dp.create_streaming_table("DimProducts")
dp.create_auto_cdc_flow(
  target = "DimProducts",
  source = "DimProducts_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type= 2
)
