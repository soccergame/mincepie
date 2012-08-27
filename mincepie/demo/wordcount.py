"""Wordcount mapper and reducers

This is a sample implementation of simple mappers and reducers that does
wordcount. See the word count demos in demos/ for details.
"""
from mincepie import mapreducer, launcher


class WordCountMapper(mapreducer.BasicMapper):
    """The class that performs wordcount map
    
    The input value of this mapper should be a string containing the words
    to be counted
    """
    def map(self, key, value):
        for word in value.split():
            yield word, 1

mapreducer.REGISTER_MAPPER(WordCountMapper)


class WordCountFromFileMapper(mapreducer.BasicMapper):
    """The class that performs wordcount map
    
    The input value of this mapper should be a string containing the filename,
    and all words in that file should be counted.
    """
    def map(self, key, value):
        with open(value,'r') as fid:
            for line in fid:
                for word in line.split():
                    yield word, 1

mapreducer.REGISTER_DEFAULT_MAPPER(WordCountFromFileMapper)


class WordCountReducer(mapreducer.BasicReducer):
    """The class that performs wordcount reduce
    """
    def reduce(self, key, value):
        return sum(value)

mapreducer.REGISTER_DEFAULT_REDUCER(WordCountReducer)


if __name__ == "__main__":
    launcher.launch()