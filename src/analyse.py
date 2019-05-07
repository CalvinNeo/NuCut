import sys

def main(fn):
    st = set()
    tot = 0
    f = open(fn, "r")
    self_circle = 0
    for l in f.readlines():
        u, v = map(int, l.split(" "))
        tot += 1
        # if "{},{}".format(u, v) in st:
        #     print "REP", "{},{}".format(u, v)
        if u == v:
            self_circle += 1
        else:
            # if u > v:
            #     u, v = v, u
            st.add("{},{}".format(u, v))
    print tot, self_circle, tot - len(st)

if __name__ == '__main__':
    main("dataset/tmin1m.txt")
