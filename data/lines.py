#coding: utf8

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

# if __name__ == '__main__':
#     f = LineFile("user_pay.txt")
#     i = 0
#     import time
#     for l in f:
#         if i % 100000 == 0:
#             print i
#         i += 1
#     print "final", i

