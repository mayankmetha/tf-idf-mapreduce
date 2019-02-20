import nltk
import sys

# get txt corpus as argument
book = open(sys.argv[1],"r")
# tokenize the txt
token = nltk.word_tokenize(book.read())
# stop word removal
stop_word = set(nltk.corpus.stopwords.words("english"))
filtered_tokens = [w for w in token if not w in stop_word]
# lemmatizing
lemmatizer = nltk.stem.WordNetLemmatizer()
final_token = [lemmatizer.lemmatize(w) for w in filtered_tokens]
print(final_token)