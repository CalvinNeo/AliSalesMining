#coding: utf8

from pymongo import *

import time

def pull_db(connection, total):
    alidb = connection.alidb
    pay_table = alidb.pay
    res = pay_table.find().sort("ts")
    for items in res:
        print items
        time.sleep(0.1)

if __name__ == '__main__':
    connection = MongoClient("mongodb://localhost:27017/")
    # clean_db(connection)
    ITEMC = 100
    print "querying the top %d items into db" % ITEMC
    pull_db(connection, ITEMC)
