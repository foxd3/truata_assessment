from pyspark.sql import SparkSession

FILE = "../files/sf-airbnb-clean.parquet/"


def create_dataframe():
    """
    Load the AirBnB parquet data file into a Spark DataFrame.

    :return: DataFrame.
    """

    spark = SparkSession.builder.appName('pyspark').getOrCreate()
    dataframe = spark.read.parquet(FILE)

    return dataframe
