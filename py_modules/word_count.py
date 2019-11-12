from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext


def do_word_counts(lines):
    """ count of words in an rdd of lines """

    counts = (lines.flatMap(lambda x: x.split())
              .map(lambda x: (x, 1))
              .reduceByKey(add)
              )
    results = {word: count for word, count in counts.collect()}
    return results


if __name__ == "__main__":
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile("file:////home/ec2-user/python_project/sample.txt")
    print(do_word_counts(lines))

