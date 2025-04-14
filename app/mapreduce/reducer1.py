#!/usr/bin/env python3

import sys
from cassandra.cluster import Cluster


def import_cassandra_vocabulary(session, term, document_frequency, document_list):
    session.execute(
        """
        INSERT INTO vocabulary (term, document_frequency, document_list)
        VALUES (%s, %s, %s)
        """,
        (term, document_frequency, document_list)
    )


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('search_engine')

    current_word = None
    current_doc_list = {}

    for line in sys.stdin:
        line = line.strip()
        word, doc_id, count = line.split('\t', 2)
        count = int(count)

        if current_word == word:
            current_doc_list[doc_id] = current_doc_list.get(doc_id, 0) + count
        else:
            if current_word:
                doc_freq = len(current_doc_list)
                import_cassandra_vocabulary(session, current_word, doc_freq, current_doc_list)

            current_word = word
            current_doc_list = {doc_id: count}

    if current_word:
        doc_freq = len(current_doc_list)
        import_cassandra_vocabulary(session, current_word, doc_freq, current_doc_list)


if __name__ == "__main__":
    main()
