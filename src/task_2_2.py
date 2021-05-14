import csv
from src.task_2_1 import create_dataframe

CSV_OUT = '../out/out_2_2.txt'


def write_min_max_csv():
    """
    Get the total row count, min price and max price from the dataframe. Write to csv file.

    :return: None
    """
    df = create_dataframe()

    row_count = df.count()

    # Get basic stats on the price data
    price_stats = df.describe("price")
    # Get the min and max prices found
    min_price = price_stats.filter("summary = 'min'").select("price").first().asDict()["price"]
    max_price = price_stats.filter("summary = 'max'").select("price").first().asDict()["price"]

    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        file = csv.writer(f)
        file.writerow(["min_price", "max_price", "row_count"])
        file.writerow([min_price, max_price, row_count])


if __name__ == "__main__":
    write_min_max_csv()
