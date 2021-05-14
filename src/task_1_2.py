from src.task_1_1 import get_entries

DISTINCT_LIST = "../out/out_1_2a.txt"
COUNT = "../out/out_1_2b.txt"


def get_unique_entries():
    """
    Create an RDD of unique entries in the source data.

    :return: PipelinedRDD
    """
    all_entries = get_entries()
    distinct_entries = all_entries.distinct()

    return distinct_entries


def write_data(data_set):
    """
    Write files containing unique entries and unique entry count.

    :param data_set: An RDD of unique items in the source data.
    :return: None
    """
    with open(DISTINCT_LIST, "w") as f:
        for i in data_set.collect():
            f.write(str(i) + "\n")

    with open(COUNT, "w") as f:
        f.write(f"Count:\n{str(data_set.count())}")

    print("Two files written successfully")


if __name__ == "__main__":
    unique_entries = get_unique_entries()
    write_data(unique_entries)
