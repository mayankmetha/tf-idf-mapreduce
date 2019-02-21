#!/usr/bin/env python3
import sys
import numpy

def reducer3():
    list_word = []
    list_doc = []
    list_tf = []
    list_idf = []
    for line in sys.stdin:
        # remove trailing and leading whitespace
        line = line.strip()
        # parse input
        word,doc,tf=line.split('\t',2)
        
        #push data to tmp storage
        list_word.append(word)
        list_doc.append(doc)
        list_tf.append(tf)
    # get D
    total_doc = len(list(set(list_doc)))
    
    current_word = None
    current_count = 0
    for _ in range(len(list_word)):
        # compute the word count
        if current_word == list_word[_]:
            current_count += 1
        else:
            list_idf.append(current_count)
            print(current_count)
            current_word = list_word[_]
            current_count = 1

    # compute idf
    idf = numpy.array(list_idf)
    numpy.log(numpy.divide(total_doc,idf))

    i = 0
    current_word = None
    for _ in range(len(list_word)):
        # compute tf-idf
        if current_word == list_word[_]:
            list_tf[_] = list_tf[_]*idf[i]
            print('%s\t%s\t%s'%(list_word[_],list_doc[_],list_tf[_]))
        else:
            current_word = list_word[_]
            i = i+1

if __name__ == '__main__':
    reducer3()