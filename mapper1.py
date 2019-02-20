#!/usr/bin/env python3
import nltk
import sys
import os
import re

def mapper1():
    for line in sys.stdin:
        # getting input stream
        words = nltk.word_tokenize(line.strip())
        # convert to lowercase
        words = [word.lower() for word in words]
        # punctuation removal
        token = [re.sub(r'[^\w\s]','',s) for s in words]
        # stop word removal
        stop_word = set(nltk.corpus.stopwords.words("english"))
        stop_word = [token.lower() for token in stop_word]
        filtered_tokens = [w for w in token if not w in stop_word]
        # lemmatizing
        lemmatizer = nltk.stem.WordNetLemmatizer()
        final_token = [lemmatizer.lemmatize(w) for w in filtered_tokens]
        # output : word corpus 1
        for word in final_token:
            print('%s\t%s\t1'%(os.getenv('mapreduce_map_input_file','noname'),word))

if __name__ == '__main__':
    mapper1()