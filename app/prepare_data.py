import os

from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()


df = spark.read.parquet("a.parquet")
n = 1000
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)


def extract_info(file_path_content):
    file_path, content = file_path_content
    file_name = os.path.basename(file_path)
    doc_id, raw_title = file_name.split('_', 1)
    doc_title = raw_title.rsplit('.', 1)[0]  # Remove the .txt extension
    return doc_id, doc_title, content.strip()


documents_rdd = spark.sparkContext.wholeTextFiles("data")
transformed_rdd = documents_rdd.map(extract_info)

columns = ["id", "title", "content"]
df = transformed_rdd.toDF(columns)

output_dir = "hdfs://localhost:9000/index/data"

df.coalesce(1).write.csv(output_dir, mode="overwrite", header=True)
