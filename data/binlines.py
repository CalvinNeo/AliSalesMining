#coding: utf8

import struct

if __name__ == '__main__':
    ITEMS = 20000
    fin = open('final.bin', 'rb')
    fout = open("part2000.txt".format(ITEMS), 'w+')
    for i in xrange(ITEMS):
        data = fin.read(12)
        ts, sid, uid = struct.unpack("=3i", data)
        fout.write("{},{},{}\n".format(uid, sid, ts))
    fout.close()

