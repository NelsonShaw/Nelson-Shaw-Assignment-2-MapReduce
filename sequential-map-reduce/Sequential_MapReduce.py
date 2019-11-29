# Import all of the necessary tools for the application.
# Please import any of these libraries if there are not immediately available to
# you.
from functools import *
from collections import *
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
import re
import pandas as pd
import time

# This function will modify a word to remove any extra characters and set the
# word to be lowercase.
def setLowercaseWord(word):
    # Return the lowercase word.
    return re.sub(r'[^\w\s]','',word).lower()

# This function will ensure that the word is not in the list of English
# prepositions.
def wordNotInPrepositions(word):
    # Return any words that are not prepositions and are alphabet words.
    return word not in ENGLISH_STOP_WORDS and word and word.isalpha()

# This function will divide a list into chunks.
def listChunk(originalList, chunkNumber):
    # Divide the list into a specified number of chunks.
    return [originalList[index::chunkNumber] for index in range(chunkNumber)]

# This function is responsible for taking each tweet text from the dataset and
# finding the frequency of each word in the text.
def mapperFunction(text):
    # Divide the text of an individual tweet into a list of words.
    tweetWords = text.split()
    
    # Take each element and set it to lowercase and remove any unnecessary characters.
    tweetWords = map(setLowercaseWord, tweetWords)
    
    # Filter the prepositions out.
    tweetWords = filter(wordNotInPrepositions, tweetWords)

    # Return the word count for each word in that particular tweet.
    return Counter(tweetWords)

# This is the reducer function that takes the mapped chunks of tweet texts and
# reduces them into one list of tuples: (Word Count, 'Word')
def reducerFunction(counter1, counter2):
    # Combine the counters together.
    counter1.update(counter2)

    # Return the counters.
    return counter1

# This function gets a chunk of tweet texts and performs MapReduce on it.
def mapChunks(chunk):
    # Map each tweet text in the chunk to the function.
    mapped = map(mapperFunction, chunk)

    # Reduce the word count arrays of each tweet text into one counter.
    reduced = reduce(reducerFunction, mapped)

    # Return the reduced/combined counter.
    return reduced


# Start the timer for the execution of the app.    
startTime = time.time()

# Read the CSV data for the app.
csvData = pd.read_csv('Donald-Tweets!.csv')
data = csvData.get("Tweet_Text")

# Check if this is the entry point of the application.
if (__name__ == '__main__'):
    # Divide the list of tweet texts into 40 chunks.
    dataChunks = listChunk(data, 40)

    # Map each chunk to the mapping function.
    mapped = map(mapChunks, dataChunks)

    # Reduce all of the chunks into one list/counter.
    reduced = reduce(reducerFunction, mapped)

    # Get the most common words.
    mostCommonWords = reduced.most_common()
    
    # Print how long it took.
    print("It took %s seconds to execute the sequential MapReduce algorithm." % (time.time() - startTime))

    # Go through each word and display the frequency of each word.
    print("\nMost common words:")
    for word in mostCommonWords:
        print(word)

    # Print how long it took.
    print("\nIt took %s seconds to execute the sequential MapReduce program." % (time.time() - startTime))


