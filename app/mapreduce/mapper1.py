#!/usr/bin/env python3

import sys
import csv
import re
from cassandra.cluster import Cluster


def import_cassandra_documents(session, document_id, title, doc_len):
    session.execute(
        """
        INSERT INTO documents (document_id, title, length)
        VALUES (%s, %s, %s)
        """,
        (document_id, title, doc_len)
    )


def tokenize(text):
    return re.findall(r'\w+', text.lower())


def main():
    reader = csv.DictReader(sys.stdin)
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('search_engine')

    for row in reader:
        doc_id = row['id']
        title = row['title']
        content = row['content']
        import_cassandra_documents(session, doc_id, title, len(content))
        words = tokenize(content)
        for word in words:
            print(f"{word}\t{doc_id}\t1")


if __name__ == "__main__":
    main()
