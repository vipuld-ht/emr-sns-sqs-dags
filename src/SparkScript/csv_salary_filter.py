from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import datetime
import random

def create_spark_session():
    return SparkSession.builder.appName("EmployeeSalaryFilter").getOrCreate()

def read_csv(spark, input_path):
    return spark.read.option("header", "true").csv(input_path)

def process_data(df, random_salary_threshold):
    df_filtered = df.withColumn("salary", col("salary").cast("int"))
    return df_filtered.filter(col("salary") > random_salary_threshold)

def write_output(df, output_path, random_salary_threshold):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    output_file = f"{output_path}filtered_employees_morethan_salary_{random_salary_threshold}.csv"
    df.write.mode("overwrite").csv(output_file, header=True)

def main():
    spark = create_spark_session()
    input_path = "s3://insightrag-job-config/input/employees.csv"
    output_path = "s3://insightrag-job-config/emr/output/"
    
    random_salary_threshold = random.randint(35000, 90000)
    employees_df = read_csv(spark, input_path)
    filtered_df = process_data(employees_df,random_salary_threshold)
    write_output(filtered_df, output_path, random_salary_threshold)
    
    spark.stop()

if __name__ == "__main__":
    main()