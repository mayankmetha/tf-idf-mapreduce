#!/usr/bin/env python3
import sys

def reducer2():
    total_count = 0
    current_doc = None
    words = []
    words_count = []
    for line in sys.stdin:
        # remove trailing and leading whitespace
        line = line.strip()
        # parse input
        doc_name,word,count=line.split('\t',2)
        try:
            count = int(count)
        except ValueError:
            continue

        #push data to tmp storage
        words.append(word)
        words_count.append(count)
        # compute the word count
        if current_doc == doc_name:
            total_count += count
        else:
            current_doc = doc_name

    # display the tf
    for _ in range(len(words)):
        print('%s\t%s\t%f'%(current_doc,words[_],(int(words_count[_])/total_count)))

if __name__ == '__main__':
    reducer2()