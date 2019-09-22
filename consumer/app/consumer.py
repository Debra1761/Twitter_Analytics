import os
import time
from kafka import KafkaConsumer
import pymongo
import json
import joblib
import json
import ast
import numpy as np
import re

kafka_topic = "tweetstream"

def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        new[key1] = val1

    return new


def standardize_text(txt):
    txt = re.sub(r"http\S+", "",txt)
    txt = re.sub(r"http", "",txt)
    txt = re.sub(r"@\S+", "",txt)
    txt = re.sub(r"[^A-Za-z0-9(),!?@\'\`\"\_\n]", " ",txt)
    txt = re.sub(r"@", "at",txt)
    txt = txt.lower()
    return txt

if __name__ == '__main__':
    print('$$$$$$$$$$$ CONSUMER $$$$$$$$$$$$$$$$')

    myclient = pymongo.MongoClient("mongodb://root:example@mongo:27017/")
    mydb = myclient["tweetbase"]
    mycol = mydb["test"]


    success = False
    consumer = None
    while success == False:
        time.sleep(1)
        try:
            consumer = KafkaConsumer(bootstrap_servers=['kafka:9092'])
            consumer.subscribe([kafka_topic])
            success = True
        except:
            print("consumer error")


    for msg in consumer:
        record = json.loads(msg.value)
        classifier = joblib.load('class.pkl')
        text = standardize_text(record['text'])
        predict = classifier.predict([text])
        print(text, predict)
        record["relevance"] = predict[0]
        record = correct_encoding(record)

        x = mycol.insert_one(record)


    #
    # # time.sleep(3)   # wait until Kafka is running
    # kafka_service = os.environ['KAFKA_SERVICE']
    # print("Consumer is using kafka service {0}".format(kafka_service))





