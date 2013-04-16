"""
Wordcount demo

This demo code shows how to perform word count from a set of files.
"""

""" Import necessary modules: You will need mapreducer to write your mapper and
reducer, and launcher to launch your program.
"""
from mincepie import mapreducer
from mincepie import launcher

""" Write our own mappers and reducers, derived from mapreducer.BasicMapper and
mapreducer.BasicReducer respectively.

For our case, the input value of the mapper is a string as the filename, and the
input key does not matter. the input key of the reducer is the word, and the 
value is a list of counts to be summed up.

Optionally, we register the mappers and reducers as default so we do not need
to specify them in the commandline arguments.
"""

class WordCountMapper(mapreducer.BasicMapper):
    def map(self, key, value):
        with open(value,'r') as fid:
            for line in fid:
                for word in line.split():
                    yield word, 1

mapreducer.REGISTER_DEFAULT_MAPPER(WordCountFromFileMapper)


class WordCountReducer(mapreducer.BasicReducer):
    def reduce(self, key, value):
        return sum(value)

mapreducer.REGISTER_DEFAULT_REDUCER(WordCountReducer)


""" Finally, the main entry: simply call launcher.launch() to start everything.
"""

if __name__ == "__main__":
    launcher.launch()
