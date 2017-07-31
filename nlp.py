import nltk

from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize



# Tokenizing - word tokenizers...... sentence tokenizers
# lexicon and corpora
# corpora is body of text, e.g. medical journals
# lexicon -  words and their meanings depending on corpora

example_sentence = 'This is an example showing off stop word filtration.'

#print(sent_tokenize(example_text))

#print(word_tokenize(example_text))



# Preprocessing
#for word in word_tokenize(example_text):
#    print(word)


stop_words = set(stopwords.words('english'))

words = word_tokenize(example_sentence)

filtered_sentence = [word for word in words if not word in stop_words]
