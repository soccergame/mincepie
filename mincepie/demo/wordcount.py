"""
Wordcount demo

This demo code shows how to perform word count from a set of files. we show how
to implement a simple mapper and reducer, and launch the mapreduce process.

To run the demo, run:
    python wordcount.py --input=zen.txt
This will execute the server and the client both locally.

Optionally, you can
run the server and client manually. On the server side, run:
    python wordcount.py --launch=server --input=zen.txt
And on the client side, simply run
    python wordcount.py --launch=client --address=SERVER_IP
where SERVER_IP is the ip or the hostname of the server. If you are running
the server and the client on the same machine, --address=SERVER_IP could be 
omitted.

Optionally, you can add the following arguments to the server side so that
the counts are written to a file instead of dumped to stdout:
    --writer=FileWriter --output=count.result
(count.result could be any filename)
"""

# (1) Import necessary modules: You need mapreducer to write your mapper and
# reducer, and launcher to launch your program.

from mincepie import mapreducer
from mincepie import launcher

# (2) Write our mappers and reducers, derived from mapreducer.BasicMapper and
# mapreducer.BasicReducer respectively.

# For our case, the input value of the mapper is a string (filename), and the
# input key does not matter. the input key of the reducer is the word, and the 
# value is a list of counts to be summed up.

# Optionally, we register the mappers and reducers as default so we do not need
# to specify them in the commandline arguments.

class WordCountMapper(mapreducer.BasicMapper):
    """The wordcount mapper"""
    def map(self, key, value):
        with open(value,'r') as fid:
            for line in fid:
                for word in line.split():
                    yield word, 1

mapreducer.REGISTER_DEFAULT_MAPPER(WordCountMapper)


class WordCountReducer(mapreducer.BasicReducer):
    """The wordcount reducer"""
    def reduce(self, key, value):
        return sum(value)

mapreducer.REGISTER_DEFAULT_REDUCER(WordCountReducer)


# (3) Finally, the main entry: simply call launcher.launch() to start
# everything.

if __name__ == "__main__":
    launcher.launch()
