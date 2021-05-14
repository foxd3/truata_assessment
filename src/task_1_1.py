from pathlib import Path
from pyspark.sql import SparkSession
import requests

ROOT_DIR = "../out/"
URL = "https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv"
FILE_NAME = "out_1_1.csv"


def get_file(url: str, file_name: str):
    """
    Download the input data file if not found.

    :return: None
    """

    file_path = ROOT_DIR + file_name
    file = Path(f"{file_path}")

    if file.exists():
        print(f"File found: {file_name}")

    else:
        print(f"File not found: {file_name}. Downloading.")

        dir_path = Path(ROOT_DIR)
        dir_path.mkdir(parents=True, exist_ok=True)

        with file.open("w", encoding="utf-8") as f:
            r = requests.get(url)
            f.write(r.text)

        print(f"Download complete. File written to {file.resolve()}")

    return str(file.resolve())


def get_entries():
    """
    Create an RDD from the source data.

    :return: PipelinedRDD
    """

    file_path = get_file(URL, FILE_NAME)

    spark = SparkSession.builder.appName('pyspark').getOrCreate()

    spark_data = spark.sparkContext.textFile(file_path)\
        .map(lambda line: line.split(","))\
        .flatMap(lambda x: x)

    return spark_data


if __name__ == "__main__":
    get_file(URL, FILE_NAME)
