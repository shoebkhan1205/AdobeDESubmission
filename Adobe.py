# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

 dbutils.fs.ls("/FileStore/tables/data.tsv")

# COMMAND ----------

hits_df=spark.read.format("csv").option("delimiter","\t").option("header", True).load("dbfs:/FileStore/tables/data.tsv")
native_site_name="esshopzilla"

# COMMAND ----------

hash_col_list=["user_agent","ip","geo_city","geo_country","geo_region"]
hits_with_hash_df=hits_df.withColumn("row_sha2", sha2(concat_ws("||", *hash_col_list), 256)).withColumn("hit_time_gmt",col("hit_time_gmt").cast('int'))
hits_with_hash_df.columns

# COMMAND ----------

hit_order_df=hits_with_hash_df.filter("pagename=='Order Complete'").\
                              withColumn("product_array", split(col("product_list"),",")). \
                              select("hit_time_gmt","row_sha2",explode("product_array").alias("prod_details")). \
                              withColumn("product_count", split(col("prod_details"),";")[2].cast('int')). \
                              withColumn("revenue_for_one", split(col("prod_details"),";")[3].cast('double')). \
                              withColumn("Revenue", expr("product_count * revenue_for_one")). \
                              select("hit_time_gmt", "row_sha2", "Revenue")
display(hit_order_df)

# COMMAND ----------

# Derive Domain name and Keyword search
hit_search_df=hits_with_hash_df.filter("referrer not like '%"+native_site_name+"%'")
hit_search_df_refined = hit_search_df.withColumn("SearchE_Derived",split(col("referrer"),".com")[0]).\
                                      withColumn("SearchE_Name",element_at(split(col("SearchE_Derived"),"\."),-1)).\
                                      withColumn("Search Engine Domain", concat(col("SearchE_Name"),lit(".com"))). \
                                      withColumn("Url_param", element_at(split(col("referrer"),"\?"),-1)). \
                                      withColumn("Search Keyword", expr("lower(CASE WHEN SearchE_Name == 'yahoo' THEN  split(split(Url_param,'p=')[1],'&')[0] ELSE split(split(Url_param,'q=')[1],'&')[0] END)")). \
                                      select("hit_time_gmt", "row_sha2", "Search Engine Domain", "Search Keyword")
display(hit_search_df_refined)

# COMMAND ----------

hit_order_df.createOrReplaceTempView("order")
hit_search_df_refined.createOrReplaceTempView("lead")

hours_from_the_order = "24"
from datetime import datetime
date_prefix=datetime.today().strftime('%Y-%m-%d')

result=spark.sql("""select `Search Engine Domain`,`Search Keyword`,SUM(`Revenue`) as Revenue from order o join  lead l 
            on o.row_sha2=l.row_sha2 and o.hit_time_gmt - (3600 * """+hours_from_the_order+""") < l.hit_time_gmt group by `Search Engine Domain`,`Search Keyword`""")
result.repartition(1).sort(desc("Revenue")).write.format("csv").option("delimiter","\t").option("header", True).mode("overwrite").save("dbfs:/FileStore/tables/"+date_prefix+"_SearchKeywordPerformance.tab")

# COMMAND ----------

display(spark.read.format("csv").option("delimiter","\t").option("header", True).load("dbfs:/FileStore/tables/"+date_prefix+"_SearchKeywordPerformance.tab"))
