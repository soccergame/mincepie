"""
The basic mapreduce class, and a few default mapper and reducers for simple
cases.

Flags defined by this module:
    --mapper, --reducer, --reader, --writer: the class names for the mapper,
        reducer, reader and writer respectively.
    --input: the input pattern that gets passed to the reader.
    --output: the output that gets passed to the writer.

Yangqing Jia, jiayq@eecs.berkeley.edu
"""

import cPickle as pickle
import gflags
import glob
import logging
import sys

# flags we are going to use
gflags.DEFINE_string("mapper", "",
                     "The mapper class for the mapreduce task")
gflags.DEFINE_string("reducer", "",
                     "The reducer class for the mapreduce task")
gflags.DEFINE_string("reader", "",
                     "The reader class for the mapreduce task")
gflags.DEFINE_string("writer", "",
                     "The reader class for the mapreduce task")
gflags.DEFINE_string("output", "",
                     "The string passed to the writer")
gflags.DEFINE_string("input", "",
                     "The input pattern.")
FLAGS = gflags.FLAGS


# Register methods
# These methods allow you to register your mapper, reducer, reader and writers
# so they are recognizable as a string.

# For example, if you have your self-defined mapper class called FooMapper, 
# call REGISTER_MAPPER(FooMapper). You will then able to get the mapper class
# by its name using the MAPPER() function like MAPPER('FooMapper').
# This allows us to specify mapper using commandline arguments.

def _register(target_dict, object_to_register):
    """The basic registerer
    """
    name = object_to_register.__name__
    if name in target_dict:
        logging.fatal("Name " + name + " already registered:")
        logging.fatal(str(target_dict))
        sys.exit(1)
    target_dict[name] = object_to_register

def _register_default(target_dict, object_to_register):
    """In addition to registering the name, we also register it as default.

    If you are tired of registering objects and setting them again in the 
    commandline arguments, this allows you to simply set them as the default 
    choice. Note that this will override the previously registered default
    object.
    """
    if not object_to_register.__name__ in target_dict:
        _register(target_dict, object_to_register)
    target_dict[_DEFAULT_NAME] = object_to_register

def _get_registered(source_dict, name):
    """Get the registered object from the dictionary
    """
    try:
        if name is None or name == "":
            return source_dict[_DEFAULT_NAME]
        else:
            return source_dict[name]
    except KeyError:
        logging.fatal("Cannot find registered name " + name + " from:")
        logging.fatal(str(source_dict))
        sys.exit(1)

# Internal dictionaries to store the registered methods
_MAPPERS  = {}
_REDUCERS = {}
_READERS  = {}
_WRITERS  = {}
_DEFAULT_NAME = '_default'
# Internal methods for registering.
REGISTER_MAPPER  = lambda x: _register(_MAPPERS,  x)
REGISTER_REDUCER = lambda x: _register(_REDUCERS, x)
REGISTER_READER  = lambda x: _register(_READERS,  x)
REGISTER_WRITER  = lambda x: _register(_WRITERS,  x)
REGISTER_DEFAULT_MAPPER  = lambda x: _register_default(_MAPPERS,  x)
REGISTER_DEFAULT_REDUCER = lambda x: _register_default(_REDUCERS, x)
REGISTER_DEFAULT_READER  = lambda x: _register_default(_READERS,  x)
REGISTER_DEFAULT_WRITER  = lambda x: _register_default(_WRITERS,  x)
MAPPER  = lambda name: _get_registered(_MAPPERS,  name)
REDUCER = lambda name: _get_registered(_REDUCERS, name)
READER  = lambda name: _get_registered(_READERS,  name)
WRITER  = lambda name: _get_registered(_WRITERS,  name)


# pylint: disable=R0922
class BasicMapper(object):
    """The basic mapper class. 
    
    All your mappers are belong to this.

    Your mapper should be derived from BasicMapper, and should at least 
    implement the map() function.
    """

    def __init__(self):
        """The default initialization: calls set_up()
        """
        self.set_up()

    def set_up(self):
        """Sets up the mapper.

        Note that no parameters are passed to set_up(). For any paramter you
        need to initialize the mapper, use argparse.ArgumentParser, and then
        pass the parameters using commandline arguments.
        """
        pass

    def map(self, key, value):
        """The map function for mapreduce.

        The input should be one key and one value. The output should be a list
        of (key, value) pairs, or a yield command that emits key value pairs.

        You should implement your own map function in your derived class.
        """
        raise NotImplementedError

REGISTER_MAPPER(BasicMapper)


class BasicReducer(object):
    """The basic reducer class. 
    
    All your reducerss are belong to this.
    """

    def __init__(self):
        """The default initialization: calls set_up()
        """
        self.set_up()

    def set_up(self):
        """Sets up the reducer.

        Note that no parameters are passed to set_up(). For any paramter you
        need to initialize the mapper, use argparse.ArgumentParser, and then
        pass the parameters using commandline arguments.
        """
        pass

    def reduce(self, key, values):
        """The reduce function for mapreduce.

        The input should be one key and one value. The output should be a list
        of (key, value) pairs, or a yield command that emits key value pairs.

        You should implement your own map function in your derived class.
        """
        raise NotImplementedError

REGISTER_REDUCER(BasicReducer)


class BasicReader(object):
    """The basic reader class

    The default BasicReader assumes that the input is a string specifying
    a certain file pattern, uses glob to retrieve a list of files, and 
    emits each filename. The key would be an index starting from 0.
    """
    def __init__(self):
        self.set_up()

    def set_up(self):
        """Sets up the reader
        """
        pass

    # pylint: disable=R0201
    def read(self, input_string):
        """Reads the input

        Input:
            input: a string obtained from commandline argument --input
        Output:
            a dictionary containing (key,value) pairs.
        The default BasicReader assumes that the input is a string specifying
        a certain file pattern, uses glob to retrieve a list of files, and 
        emits each filename. The key would be an index starting from 0.
        """
        inputlist = glob.glob(input_string)
        inputlist.sort()
        return dict(enumerate(inputlist))

# If the user does not override the reader option, BasicReader is the default
# reader.
REGISTER_DEFAULT_READER(BasicReader)


class BasicWriter(object):
    """The basic writer class

    Different from the mapper and reducer base classes, you can directly
    use BasicWriter - it simply spits all the dictionary entries.
    """
    def __init__(self):
        self.set_up()
    
    def set_up(self):
        """Sets up the writer
        """
        pass

    # pylint: disable=R0201
    def write(self, result):
        """Writes the result

        The BasicWriter write() function simply dumps the key, value pairs
        to the stdout. You can write your own writer to do more fancy stuff.
        Input:
            a dictionary containing (key,value) pairs.
        """
        for key in result:
            print repr(key), ":", repr(result[key])

# If the user does not override the writer option, BasicWriter is the default
# writer.
REGISTER_DEFAULT_WRITER(BasicWriter)


class IdentityMapper(BasicMapper):
    """IdentityMapper is a mapper that simply emits the same key value pair
    """

    def map(self, key, value):
        yield key, value

REGISTER_MAPPER(IdentityMapper)


class IdentityReducer(BasicReducer):
    """IdentityReducer is a reducer that simply emits the same key value pair
    """

    def reduce(self, key, values):
        return values

REGISTER_REDUCER(IdentityReducer)


class SumReducer(BasicReducer):
    """SumReducer is a reducer that returns the sum of the values
    """
    
    def reduce(self, key, values):
        return sum(values)

REGISTER_REDUCER(SumReducer)


class FirstElementReducer(BasicReducer):
    """FirstElementReducer is a reducer that takes the first value and ignores
    others
    """
    
    def reduce(self, key, values):
        return values[0]

REGISTER_REDUCER(FirstElementReducer)


class NoPassReducer(BasicReducer):
    """This reducer returns nothing.
    
    "You shall not pass!" - Gandalf the Grey
    """

    def reduce(self, key, values):
        return

REGISTER_REDUCER(NoPassReducer)


class FileReader(BasicReader):
    """This reader reads the content of the input files, and put each line as
    a value. The key is in the format filename:lineid
    """
    def read(self, input_string):
        inputlist = glob.glob(input_string)
        inputlist.sort()
        data = {}
        for filename in inputlist:
            with open(filename, 'r') as fid:
                for index, line in enumerate(fid):
                    data[filename+":"+str(index)] = line.strip()
        return data

REGISTER_READER(FileReader)


class IterateReader(BasicReader):
    """This reader treats the input as a number, and creates range(number)
    as both the keys and the values
    """
    def read(self, input_string):
        try:
            num = int(input_string)
        except ValueError, e:
            logging.error("Unrecognized input: %s." % input_string)
            # return an empty dict so nothing gets executed.
            return dict()
        # if num is negative, set it to 0
        if num < 0:
            logging.error("Negative input: %s" % input_string)
            num = 0
        data = dict((n, n) for n in range(num))
        return data

REGISTER_READER(IterateReader)


class FileWriter(BasicWriter):
    """The class that dumps the key values pair to FLAGS.output as strings
    """
    def write(self, result):
        with open(FLAGS.output,'w') as fid:
            for key in result:
                fid.write(repr(key) + ":" + repr(result[key])+'\n')

REGISTER_WRITER(FileWriter)


class PickleWriter(BasicWriter):
    """The class that dumps the key values pair to FLAGS.output as picked
    objects.
    """
    def write(self, result):
        with open(FLAGS.output,'w') as fid:
            pickle.dump(result, fid)

REGISTER_WRITER(PickleWriter)

if __name__ == "__main__":
    print __doc__

