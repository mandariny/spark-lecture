from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

def setup_course_id_chapter_id_rdd(sc: SparkContext, is_test: bool):
    # (course_id, chapter_id)
    if is_test:
        data = [
            (1, 96),
            (1, 97),
            (1, 98),
            (2, 99),
            (3, 100),
            (3, 101),
            (3, 102),
            (3, 103),
            (3, 104),
            (3, 105),
            (3, 106),
            (3, 107),
            (3, 108),
            (3, 109),
        ]
        return sc.parallelize(data)

    return sc.textFile("data/chapters.csv") \
        .map(lambda row: row.split(",")) \
        .map(lambda row: (int(row[0]), int(row[1])))

def setup_user_id_chapter_id_rdd(sc: SparkContext, is_test: bool):
    # (user_id, chapter_id)
    if is_test:
        data = [
            (14, 96),
            (14, 97),
            (13, 96),
            (13, 96),
            (13, 96),
            (14, 99),
            (13, 100),
        ]
        return sc.parallelize(data)

    return sc.textFile("data/views-*.csv") \
        .map(lambda row: row.split(",")) \
        .map(lambda row: (int(row[0]), int(row[1])))

def setup_course_id_title_rdd(sc: SparkContext, is_test: bool):
    # (course_id, title)
    if is_test:
        data = [
            (1, "Flutter programming"),
            (2, "Marketing 101"),
            (3, "Apache Spark guide")
        ]
        return sc.parallelize(data)

    return sc.textFile("data/titles.csv") \
        .map(lambda row: row.split(",")) \
        .map(lambda row: (int(row[0]), row[1]))

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
                .master("local") \
                .appName("rdd examples ver") \
                .getOrCreate()
    sc: SparkContext = ss.sparkContext

    is_test = True

    # 0. call input data
    course_chapter_rdd = setup_course_id_chapter_id_rdd(sc, is_test)
    user_chapter_rdd = setup_user_id_chapter_id_rdd(sc, is_test)
    course_title_rdd = setup_course_id_title_rdd(sc, is_test)

    # 1. count chapter in each course
    # output : (course_id, chapter_count)
    chapter_count_per_course_rdd = course_chapter_rdd \
        .map(lambda x: (x[0], 1)) \
        .reduceByKey(lambda c1, c2: c1 + c2)
    print(f"chapter_count_per_course_rdd => {chapter_count_per_course_rdd.collect()}")

    # 2. remove duplicate data
    # output : {user_id, chapter_id}
    step2 = user_chapter_rdd.distinct()
    print(f"step2 => {step2.collect()}")

    # 3. chapter_id, user_id join
    # output : {chapter_id, (user_id, course_id))
    step3 = step2.map(lambda x: (x[1], x[0])).join(course_chapter_rdd.map(lambda x: (x[1], x[0])))
    print(f"step3 => {step3.collect()}")

    # 4. count user's course
    # output : ((user_id, course_id), view_count)
    step4 = step3.map(lambda x: (x[1], 1)).reduceByKey(lambda c1, c2: c1 + c2)
    print(f"step4 => {step4.collect()}")

    # 5. remove user_id
    # output : (course_id, view_count)
    step5 = step4.map(lambda x: (x[0][1], x[1]))
    print(f"step5 => {step5.collect()}")

    # 6. join #5 result with #1 result
    # output : (course_id, (view_count, chapter_count))
    step6 = step5.join(chapter_count_per_course_rdd)
    print(f"step6 => {step6.collect()}")

    # 7. calc ratio per course
    # output : (course_id, view_count / chapter_cout)
    step7 = step6.map(lambda x: (x[0], x[1][0] / x[1][1]))
    print(f"step7 => {step7.collect()}")

    # 8. transform ratio to score
    # output : (course_id, score)
    def calc_score(row: tuple[int, int]):
        ratio = row[1]
        if ratio >= 0.9 :
            return (row[0], 10)
        if ratio >= 0.5 :
            return (row[0], 4)
        if ratio >= 0.25 :
            return (row[0], 2)
        return row[0], 0

    step8 = step7.map(calc_score)
    print(f"step8 => {step8.collect()}")

    # 9. evaluate mean of each course
    # output : (course_id, final_score)

    # performance win!
    step9 = step8.mapValues(lambda v: (v, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda v: v[0] / v[1])

    # use groupByKey
    # step9 = step8.groupByKey().mapValues(lambda x: sum(x) / len(x))
    # print(f"step9 => {step9.collect()}")

    # 10. title match to course
    # output : (title, score)
    step10 = step9.join(course_title_rdd) \
        .map(lambda v: (v[1][1], v[1][0]))
    print(f"step10 => {step10.collect()}")

    # 11. order by score desc
    result = step10.collect()
    result.sort(key= lambda v: v[1], reverse=True)
    print("==== final result ====")
    for title, score in result:
        print(f"{title} {score}")