import csv
from src.task_2_1 import create_dataframe

CSV_OUT = "../out/out_2_3.txt"


def write_mean_csv():
    """
    Get the average number of bedrooms and bathrooms from the dataframe (price>5000, review==10). Write to csv file.

    :return: None
    """
    df = create_dataframe()

    filter_1 = df.filter(df.price > 5000)
    filtered = filter_1.filter(filter_1.review_scores_accuracy == 10)

    bathroom_stats = filtered.describe("bathrooms")
    bedroom_stats = filtered.describe("bedrooms")

    avg_bathrooms = bathroom_stats.filter("summary = 'mean'").select("bathrooms").first().asDict()["bathrooms"]
    avg_bedrooms = bedroom_stats.filter("summary = 'mean'").select("bedrooms").first().asDict()["bedrooms"]

    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        file = csv.writer(f)
        file.writerow(["avg_bathrooms", "avg_bedrooms"])
        file.writerow([avg_bathrooms, avg_bedrooms])


if __name__ == "__main__":
    write_mean_csv()
