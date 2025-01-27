# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

start_date = "6/1/2023"
end_date = "6/30/2023"

# Initialize Spark session
spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

# Function to load datasets
def load_dataset(file_path: str) -> DataFrame:
    return (spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(file_path))

# Function to convert date datatype
def convert_date_format(df: DataFrame) -> DataFrame:
    return df.withColumn('date', to_date(col('date'), "M/d/yyyy"))

# Function to create output dataset with date sequence
def create_date_sequence(start_date: str, end_date: str) -> DataFrame:
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark.createDataFrame([{"date": 1}]).select(
        explode(sequence(to_date(lit(start_date), "M/d/yyyy"), to_date(lit(end_date), "M/d/yyyy"), expr("INTERVAL 1 DAY"))).alias("date"))

# Function to join output dataset with inputs
def join_datasets(silos_df: DataFrame, histavg_df: DataFrame) -> DataFrame:
    def _join_datasets(df: DataFrame) -> DataFrame:
    
        df = df.join(silos_df.withColumn('date',to_date(col('date'),"M/d/yyyy")), on='date', how='left')
        df = df.withColumn("day", date_format(col("date"), "EEEE"))
        

        return df.join(histavg_df, on='day', how='left')
    return _join_datasets

# Function to fill gaps with average tons
def fill_gaps_with_average() -> DataFrame:
    def _fill_gaps_with_average(df: DataFrame) -> DataFrame:
        return df.withColumn('silo_wt_in_tons', when(col("silo_wt_in_tons").isNull(), col("average_tons")).otherwise(col("silo_wt_in_tons")))
    return _fill_gaps_with_average

# Function to calculate KPIs
def calculate_kpis() -> DataFrame:
    def _calculate_kpis(df: DataFrame) -> DataFrame:
        windowspec_7day = Window.partitionBy(window("date", "7 days")).orderBy(col("date"))
        windowspec_daily = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        df = df.withColumn("weekly_total_tons", when(col('day')=='Wednesday', sum(col("silo_wt_in_tons")).over(windowspec_7day)))
        df = df.withColumn("monthly_grand_total", sum(col('silo_wt_in_tons')).over(Window.partitionBy()))
        return df.withColumn("mtd_running_total_tons", sum(col("silo_wt_in_tons")).over(windowspec_daily))
    return _calculate_kpis

# Function to format output
def format_output() -> DataFrame:
    def _format_output(df: DataFrame) -> DataFrame:
        df = df.drop('day', 'average_tons')
        df = df.withColumn("weekly_total_tons", col('weekly_total_tons').cast(StringType()))
        df = df.na.fill(value='', subset=["weekly_total_tons"])
        
        return df.withColumn("date", date_format("date", "M/d/yyyy"))
    return _format_output

# Function to write output
def write_output(output_path: str) -> None:
    def _write_output(df: DataFrame) -> None:
        df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
    return _write_output

# Load datasets
silos_actuals_input_path = "dbfs:/FileStore/spark/silo_actuals.csv"
historical_averages_file_path = 'dbfs:/FileStore/spark/historical_averages.csv'
output_path = "dbfs:/FileStore/spark/result"

silos_df = load_dataset(silos_actuals_input_path)
histavg_df = load_dataset(historical_averages_file_path)

# Create date sequence DataFrame
date_sequence_df = create_date_sequence(start_date, end_date)

# Apply transformations using DataFrame.transform
transformed_df = (date_sequence_df
                  .transform(join_datasets(silos_df, histavg_df))
                  .transform(fill_gaps_with_average())
                  .transform(calculate_kpis())
                  .transform(format_output())).select("date",'silo_wt_in_tons','weekly_total_tons','mtd_running_total_tons','monthly_grand_total')

# Write the output
write_output(output_path)(transformed_df)
#display(transformed_df)
transformed_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)

