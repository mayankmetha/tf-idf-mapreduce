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
        
        # push data to tmp storage
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
            for i in range(current_count):
                list_idf.append(current_count)
            current_word = list_word[_]
            current_count = 1
    list_idf.append(current_count)

    # compute idf
    idf = numpy.array(list_idf)
    numpy.seterr(divide='ignore', invalid='ignore')
    numpy.log(numpy.divide(total_doc,idf))

    for x in range(len(list_word)):
        # compute tf-idf
        print('%s\t%s\t%f'%(list_word[x],list_doc[x],(float(list_tf[x])*float(idf[x]))))

if __name__ == '__main__':
    reducer3()