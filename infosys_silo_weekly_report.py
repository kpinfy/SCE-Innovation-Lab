################################################################################
# Script Name: infosys_silo_weekly_report.py
# Purpose: Generate the weekly Silo report by sampling actual Silo readings and
#          calculating historical Silos reading averages (for days in the week).
#
# Source datasets: historical_averages.csv, silo_actuals.csv
# Target dataset: transformed_df
# Last Modified By: Infosys Team
# Last Modified Date: Feb 25, 2024
# Version: 1.0
# Version Comment: Final version
################################################################################
# Define the start and end dates
start_date = "6/1/2023"
end_date = "6/30/2023"
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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
        df = df.join(silos_df, on='date', how='left')
        df = df.withColumn("day", date_format(col("date"), "EEEE"))
        return df.join(histavg_df, on='day', how='left')
    return _join_datasets

# Function to fill gaps with average tons
def fill_gaps_with_average(df: DataFrame) -> DataFrame:
    return df.withColumn('silo_wt_in_tons', when(col("silo_wt_in_tons").isNull(), col("average_tons")).otherwise(col("silo_wt_in_tons")))


# Function to calculate KPIs
def calculate_kpis(df: DataFrame) -> DataFrame:
    windowspec_7day = Window.partitionBy(window("date", "7 days")).orderBy(col("date"))
    windowspec_daily = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = df.withColumn("weekly_total_tons", when(col('day')=='Wednesday', sum(col("silo_wt_in_tons")).over(windowspec_7day)))
    df = df.withColumn("monthly_grand_total", sum(col('silo_wt_in_tons')).over(Window.partitionBy()))
    return df.withColumn("daily_total_tons", sum(col("silo_wt_in_tons")).over(windowspec_daily))


# Function to format output
def format_output(df: DataFrame) -> DataFrame:
    df = df.drop('day', 'average_tons')
    df = df.withColumn("weekly_total_tons", col('weekly_total_tons').cast(StringType()))
    df = df.na.fill(value='', subset=["weekly_total_tons"])
    return df.withColumn("date", date_format("date", "M/d/yyyy"))


# Function to write output
def write_output(df: DataFrame, output_path: str) -> None:
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)


# Load datasets
silos_actuals_input_path = "dbfs:/FileStore/spark/silo_actuals.csv"
historical_averages_file_path = 'dbfs:/FileStore/spark/historical_averages.csv'
output_path = "dbfs:/FileStore/spark/result"

silos_df = load_dataset(silos_actuals_input_path)
histavg_df = load_dataset(historical_averages_file_path)

# Create date sequence DataFrame
date_sequence_df = create_date_sequence("start_date", "end_date")

# Apply transformations using DataFrame.transform
transformed_df = (date_sequence_df
                  .transform(join_datasets(silos_df, histavg_df))
                  .transform(fill_gaps_with_average)
                  .transform(calculate_kpis)
                  .transform(format_output))


# Write the output
#write_output(output_path)(transformed_df)
display(transformed_df)

