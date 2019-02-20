#!/usr/bin/env python3
import sys

def reducer1():
    current_word = None
    current_count = 0
    current_doc = None
    word = None
    doc_name = None
    for line in sys.stdin:
        # remove trailing and leading whitespace
        line = line.strip()
        # parse input
        doc_name,word,count=line.split('\t',2)
        try:
            count = int(count)
        except ValueError:
            continue

        # compute the word count
        if current_word == word and current_doc == doc_name:
            current_count += count
        else:
            if current_word and current_doc:
                print('%s\t%s\t%s'%(current_doc,current_word,current_count))
            current_count = count
            current_doc = doc_name
            current_word = word

    # display the last word count
    if current_word == word and current_doc == doc_name:
        print('%s\t%s\t%s'%(current_doc,current_word,current_count))

if __name__ == '__main__':
    reducer1()