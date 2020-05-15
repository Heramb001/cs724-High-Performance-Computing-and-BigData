#!/usr/bin/env python

import sys

prev = 0
for pair in sys.stdin:
	if (pair!=prev):
		print pair
		prev= pair