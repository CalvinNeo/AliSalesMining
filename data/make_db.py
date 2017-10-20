#coding: utf8

from pymongo import *


class LineFile(object):
    def __init__(self, fn):
        self.f = open(fn, "r")

    def __del__(self):
        self.f.close()

    def __iter__(self):
        return self

    def next(self):
        while True:
            l = self.f.readline()
            if not l:
                raise StopIteration()
            else:
                l = l.strip(" ").strip("\n")
                if l == "":
                    continue
                else:
                    return l

def init_db(connection, maximun_lines = 1000):
    alidb = connection.alidb
    pay_table = alidb.pay
    # user_id, Shop_id, time_stamp
    percentile = maximun_lines / 100
    import time
    f = LineFile("user_pay.txt")
    for i in xrange(maximun_lines):
        if i % percentile == 0:
            print "completing percent %d " % int(i / percentile)
        try:
            l = f.next()
            uid, sid, ts_str = l.split(",")
            uid = int(uid); sid = int(sid)
            tsobj = time.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            ts = time.mktime(tsobj)
            pay_table.insert({'uid': uid, "sid": sid, "ts": ts})
        except StopIteration:
            break

def clean_db(connection):
    alidb = connection.alidb
    pay_table = alidb.pay
    pay_table.drop()

if __name__ == '__main__':
    connection = MongoClient("mongodb://localhost:27017/")
    # clean_db(connection)
    ITEMC = 1000
    clean_db(connection)
    print "inserting the top %d items into db" % ITEMC
    init_db(connection, ITEMC)
