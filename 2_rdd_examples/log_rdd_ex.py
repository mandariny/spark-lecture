from typing import List
from datetime import datetime

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("rdd examples ver") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    log_rdd: RDD[str] = sc.textFile("data/log.txt")

    # print(log_rdd.count())
    # log_rdd.foreach(print)

    # a) map
    # a-1) get each line of log.txt to List[str]
    def parse_line(row: str):
        return row.strip().split(" | ")

    parsed_log_rdd: RDD[List[str]] = log_rdd.map(parse_line)

    # parsed_log_rdd.foreach(print)

    # b) filter
    # b-1) get log whose http status code is 404
    def get_only_404(row: List[str]):
        return row[-1] == '404'

    rdd_404 = parsed_log_rdd.filter(get_only_404)
    # rdd_404.foreach(print)

    # b-2) get normal http status in logs
    def get_only_2xx(row: List[str]):
        status_code = row[-1]
        return status_code.startswith("2")

    rdd_2xx = parsed_log_rdd.filter(get_only_2xx)
    # rdd_2xx.foreach(print)

    # b-3) POST request & request path is /playbooks
    def get_post_request_and_playbooks_api(row: List[str]):
        log = row[2].replace("\"", "")
        return log.startswith("POST") and "/playbooks" in log

    rdd_playbooks = parsed_log_rdd.filter(get_post_request_and_playbooks_api)
    # rdd_playbooks.foreach(print)

    # c) reduce
    # c-1) count each API method
    def extract_api_method(row: List[str]):
        log = row[2].replace("\"", "")
        api_method = log.split(" ")[0]
        return api_method, 1

    rdd_count_by_api_method = parsed_log_rdd.map(extract_api_method) \
        .reduceByKey(lambda c1, c2: c1 + c2) \
        .sortByKey()

    # rdd_count_by_api_method.foreach(print)

    # c-2) count request in minutes
    def extract_hour_and_minute(row: List[str]):
        timestamp = row[1].replace("[", "").replace("]", "")
        date_format = "%d/%b/%Y:%H:%M:%S"
        date_time_obj = datetime.strptime(timestamp, date_format)
        return f"{date_time_obj.hour}:{date_time_obj.minute}", 1

    rdd_count_by_minute = parsed_log_rdd.map(extract_hour_and_minute) \
        .reduceByKey(lambda c1, c2: c1 + c2) \
        .sortByKey()

    # rdd_count_by_minute.foreach(print)

    # d) group by
    # d-1) print ip list by status code and api method
    def extract_cols(row: List[str]) -> tuple[str, str, str]:
        ip = row[0]
        status_code = row[-1]
        api_log = row[2].replace("\"", "")
        api_method = api_log.split(" ")[0]
        return status_code, api_method, ip

    # group by
    parsed_log_rdd.map(extract_cols) \
        .map(lambda x: ((x[0], x[1]), x[2])) \
        .groupByKey().mapValues(list) \
        # .foreach(print)

    # reduce by
    parsed_log_rdd.map(extract_cols) \
        .map(lambda x: ((x[0], x[1]), x[2])) \
        .reduceByKey(lambda i1, i2: f"{i1}, {i2}") \
        .map(lambda row: (row[0], row[1].split(","))).foreach(print)