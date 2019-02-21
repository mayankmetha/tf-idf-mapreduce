#!/usr/bin/env python3
import sys

def mapper3():
    for line in sys.stdin:
        line = line.strip()
        doc_name,word,count=line.split('\t',2)
        print('%s\t%s\t%s'%(word,doc_name,count))

if __name__ == '__main__':
    mapper3()