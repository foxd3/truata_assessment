from src.task_2_1 import create_dataframe

FILE = "../out/out_2_4.txt"


def write_guests_csv():
    """
    Get the number of guests that the highest rated and lowest priced property can accommodate. Write to txt file.

    :return: None
    """
    df = create_dataframe()

    min_price = df.describe("price").filter("summary = 'min'").select("price").first().asDict()["price"]
    max_review_scores_rating = df.describe("review_scores_rating").filter("summary = 'max'")\
        .select("review_scores_rating").first().asDict()["review_scores_rating"]

    property_entry = df.filter(df.review_scores_rating == max_review_scores_rating).filter(df.price == min_price)
    guests = property_entry.select("accommodates").first().asDict()["accommodates"]

    with open(FILE, "w", newline="", encoding="utf-8") as f:
        f.write(str(int(guests)))


if __name__ == '__main__':
    write_guests_csv()
