#!/usr/bin/env python

import sys

sys.path.append('.')

a=[]
val=[]
#l1=[]

for  i in sys.stdin:
        i = i.strip()
        items = i.split()
        a.append(items)
a = map(set,a)

for line in open('phs1out','r'):
    l1 = line.strip()
    if l1:
        l1=l1.split()
        val.append(l1)
val = map(frozenset,val)

for i in a:
    for j in val:
            if j.issubset(i):
                    x,y=j
                    print "(%s %s),1"%(x,y)