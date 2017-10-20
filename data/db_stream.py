#coding: utf8

from pymongo import *
import itertools

import time

def pull_db(connection, total):
    alidb = connection.alidb
    pay_table = alidb.pay
    res = pay_table.find().sort("ts")
    percentile = total / 100
    for (i, item) in itertools.izip(xrange(total), res):
        if i % percentile == 0:
            print "completing percent %d " % int(i / percentile)
        print item

if __name__ == '__main__':
    connection = MongoClient("mongodb://localhost:27017/")
    # clean_db(connection)
    ITEMC = 100
    print "querying the top %d items into db" % ITEMC
    pull_db(connection, ITEMC)
