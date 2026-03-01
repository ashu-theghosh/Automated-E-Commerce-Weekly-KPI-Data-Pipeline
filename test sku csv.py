# Databricks notebook source
from datetime import datetime, timezone
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

# COMMAND ----------

base_path="/Volumes/workspace/default/ecomm_project/"

bronze_incoming_path=f"{base_path}bronze/incoming/"
bronze_processed=f"{base_path}bronze/processed/"

silver_path=f"{base_path}silver/clean_sku_data/"

gold_date_wise_sku_count_summary=f"{base_path}gold/date_wise_sku_count_summary/"
gold_approve_reject_count_percent=f"{base_path}gold/approve_reject_count_percent/"
gold_top_rejection_reason_count_percent=f"{base_path}gold/top_rejection_reason_count_percent/"
gold_category_percent=f"{base_path}gold/category_percent/"
gold_team_productivity=f"{base_path}gold/team_productivity/"


# COMMAND ----------

# List files
files=dbutils.fs.ls(bronze_incoming_path)

# Filter only CSV files
csv_files=[f.path for f in files if f.path.endswith(".csv")]

if not csv_files:
    print("No new CSV files found. Exiting pipeline.")
else:
    print("Files detected for processing:")
    for f in csv_files:
        path=f
        c_path=path.split(":")[1]
        print(c_path)

        df=spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(c_path)
        print(df.columns)
        df.display()
        df.printSchema()

        #clean full dataframe
        new_df=df.withColumn("date",col("processed_datetime").cast("date"))\
            .withColumn("week_start_date",date_add(date_trunc("week", date_sub(col("processed_datetime"), 2)),2).cast("date"))

        new_df.write\
          .mode("append")\
          .partitionBy("week_start_date")\
          .parquet(silver_path)
        new_df.display()
        
        #date-wise sku count
        date_wise_sku_df=new_df.groupBy(col("week_start_date"),col("date")).agg(count("*").alias("sku_count")).orderBy(col("date"))\
                         .withColumn("load_timestamp",current_timestamp())
        date_wise_sku_df.display()
        date_wise_sku_df.write\
          .mode("append")\
          .partitionBy("week_start_date")\
          .parquet(gold_date_wise_sku_count_summary)
        
        total_skus=new_df.count()
        print(total_skus)
        
        #approve_reject_total_percent
        approved_rejected_grouped_df=new_df.groupBy(col("week_start_date"),col("status")).agg(count("*").alias("sku_count")).orderBy(col("status"))   
        approved_rejected_percent_df=approved_rejected_grouped_df.withColumn("percent",round((col("sku_count")/total_skus)*100,2))\
                                     .withColumn("load_timestamp",current_timestamp())
        approved_rejected_percent_df.write\
          .mode("append")\
          .partitionBy("week_start_date")\
          .parquet(gold_approve_reject_count_percent)
        approved_rejected_percent_df.display()
        
        #scaler_rejected_count_calculation
        rejection_count_df=approved_rejected_percent_df.where(col("status")=="Rejected").select(col("sku_count"))
        rejection_count=rejection_count_df.collect()[0][0]
        print(rejection_count)
        
        #top rejection reasons
        top_rejection_reasons_df=new_df.where(col("status")=="Rejected").groupBy(col("week_start_date"),col("rejection_reason")).agg(count("*").alias("sku_count")).orderBy(col("sku_count").desc())
        top_rejection_reasons_df_with_percent=top_rejection_reasons_df.withColumn("percent",round((col("sku_count")/rejection_count)*100,2))\
                                      .withColumn("load_timestamp",current_timestamp())
        top_rejection_reasons_df_with_percent.write\
          .mode("append")\
          .partitionBy("week_start_date")\
          .parquet(gold_top_rejection_reason_count_percent)
        top_rejection_reasons_df_with_percent.display()
        
        
        #category_percent_share_for_rejection_reason
        window_spec=Window.partitionBy(col("week_start_date"), col("category"))
        window_spec_one=Window.partitionBy(col("week_start_date"), col("rejection_reason"))
        rejected_df=new_df.where(col("status")=="Rejected")

        grouped_df=rejected_df.groupBy(col("week_start_date"), col("category"), col("rejection_reason")) \
                         .agg(count("*").alias("reason_count"))
        category_percent_df=grouped_df.withColumn("category_total",sum("reason_count").over(window_spec))\
                  .withColumn("reason_total",sum("reason_count").over(window_spec_one))\
                  .withColumn("percent",round((col("reason_count")/col("reason_total"))*100,2)).orderBy(col("reason_total").desc(),col("percent").desc())\
                  .withColumn("load_timestamp",current_timestamp())
        category_percent_df.write\
          .mode("append")\
          .partitionBy("week_start_date")\
          .parquet(gold_category_percent)
        category_percent_df.display()
        
        #teams_productivity
        productivity_df=new_df.groupBy(col("week_start_date"),col("processed_by")).agg(count("*").alias("sku_count")).orderBy(col("sku_count").desc())\
                        .withColumn("load_timestamp",current_timestamp())
        productivity_df.write\
          .mode("append")\
          .partitionBy("week_start_date")\
          .parquet(gold_team_productivity)
        productivity_df.display()
        dbutils.fs.mv(c_path, bronze_processed+c_path.split("/")[-1])
print("Pipeline completed successfully.")
    