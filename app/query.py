from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster
import sys
import math


def bm25_score(doc_freq, term_freq, doc_length, avg_doc_length, N, k1=1, b=0.75):
    idf = math.log((N - doc_freq + 0.5) / (doc_freq + 0.5) + 1)
    score = idf * ((term_freq * (k1 + 1)) / (term_freq + k1 * ((1 - b) + b * (doc_length / avg_doc_length))))
    return score


def query_index(query_terms, spark_context):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('search_engine')

    vocabulary_rdd = spark_context.parallelize(session.execute("SELECT * FROM vocabulary"))
    documents_rdd = spark_context.parallelize(session.execute("SELECT * FROM documents"))

    N = documents_rdd.count()

    total_length = documents_rdd.map(lambda doc: doc.length).sum()
    avg_doc_length = total_length / N if N > 0 else 0

    documents_rdd = documents_rdd.map(lambda doc: (doc.document_id, (doc.title, doc.length)))

    scores_rdd = vocabulary_rdd \
        .filter(lambda voc: voc.term in query_terms) \
        .flatMap(lambda voc:
                 [
                     (doc_id, (term_freq, voc.document_frequency)) for doc_id, term_freq in voc.document_list.items()
                 ]
                 ) \
        .join(documents_rdd) \
        .map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1][1])) \
        .map(lambda x:
             (
                 x[0],
                 bm25_score(x[2], x[1], x[3], avg_doc_length, N)
             )
        )

    scores = scores_rdd.reduceByKey(lambda a, b: a + b)

    results = scores.join(documents_rdd) \
        .sortBy(lambda x: -x[1][0]) \
        .map(lambda x: (x[0], x[1][0], x[1][1][0])) \
        .take(10)

    return results


def main():
    query = " ".join(sys.argv[1:]).strip()
    query_terms = query.lower().split()

    spark_conf = SparkConf().setAppName("BM25QueryApp")
    sc = SparkContext(conf=spark_conf)

    top_documents = query_index(query_terms, sc)

    for doc_id, score, title in top_documents:
        print(f"Document ID: {doc_id}, Title: {title}, Score: {score:.2f}")

    sc.stop()


if __name__ == '__main__':
    main()
