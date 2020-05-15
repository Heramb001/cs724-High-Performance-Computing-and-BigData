#!/usr/bin/env python
import sys
from apriori import createC1
from apriori import scanD
from apriori import aprioriGen

a=[]
for i in sys.stdin:
    i = i.strip()
    v = i.split()
    a.append(v)
c1 = createC1(a)
d = map(set,a)

K=2
ps = 0.3

for i in range(K):
    L1,sp = scanD(d,c1,ps)
    c1 = aprioriGen(L1,i+1)

for i in L1:
    x,y = i
    x = int(x)
    y = int(y)
    print "%d %d" %(x,y)