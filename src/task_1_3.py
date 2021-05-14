from src.task_1_2 import get_entries

FILE = "../out/out_1_3.txt"


def top_five_count():
    """
    Find the top five purchased products and how often they were purchased.

    :return: None
    """

    entries = get_entries()
    count_dict = entries.countByValue()
    count_tuples = count_dict.items()
    sorted_data = sorted(count_tuples, key=lambda entry: entry[1], reverse=True)
    top_five = sorted_data[:5]

    with open(FILE, "w") as f:
        for item in top_five:
            f.write(str(item) + "\n")

    print("Top five purchases and their frequency written to file.")


if __name__ == '__main__':
    top_five_count()
