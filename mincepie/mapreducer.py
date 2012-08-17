"""Basic mapreduce class, and a few default ones

author: Yangqing Jia (jiayq84@gmail.com)
"""


# Register methods
# These methods allow you to register your mapper, reducer, reader and writers
# so they are recognizable as a string.
#
# For example, if you have your self-definedmapper class called FooMapper, 
# call REGISTER_MAPPER(FooMapper). You will then able to get the mapper class
# by its name using the MAPPER() function like MAPPER('FooMapper').
# This allows us to specify mapper using commandline arguments.

def _REGISTER(target_dict, object_to_register):
    """The basic registerer
    """
    target_dict[object_to_register.__name__] = object_to_register

_MAPPERS  = {}
_REDUCERS = {}
_READERS  = {}
_WRITERS  = {}
REGISTER_MAPPER  = lambda mapper:  _REGISTER(_MAPPERS,  mapper)
REGISTER_REDUCER = lambda reducer: _REGISTER(_REDUCERS, reducer)
REGISTER_READER  = lambda reader:  _REGISTER(_READERS,  reader)
REGISTER_WRITER  = lambda writer:  _REGISTER(_WRITERS,  writer)

def Mapper(name):
    """Get Mapper by its name

    See REGISTER_MAPPER() for details
    """
    return _MAPPERS[name]

def Reducer(name):
    """Get Reducer by its name

    See REGISTER_MAPPER() for details
    """
    return _REDUCERS[name]

def Reader(name):
    """Get Reader by its name

    See REGISTER_MAPPER() for details
    """
    return _READERS[name]

def Writer(name):
    """Get Reader by its name

    See REGISTER_MAPPER() for details
    """
    return _WRITERS[name]


class BasicMapper(object):
    """The basic mapper class. 
    
    All your mappers are belong to this.
    """

    def __init__(self):
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

    All your readers are belong to this.
    """
    def __init__(self):
        self.set_up()

    def set_up(self):
        """Sets up the reader
        """
        pass

    def read(self, inputlist):
        """Reads the inputlist

        Input:
            inputlist: a list of strings obtained from commandline arguments
        Output:
            a dictionary containing (key,value) pairs.
        """
        raise NotImplementedError

REGISTER_READER(BasicReader)


class BasicWriter(object):
    """The basic writer class

    Different from the mapper, reducer, reader base classes, you can directly
    use BasicWriter - it simply spits all the dictionary entries.
    """
    def __init__(self):
        self.set_up()
    
    def set_up(self):
        """Sets up the writer
        """
        pass

    def write(self, result):
        """Writes the result

        The BasicWriter write() function simply dumps the key, value pairs
        to the stdout. You can write your own writer to do more fancy stuff.
        Input:
            a dictionary containing (key,value) pairs.
        """
        for key in result:
            print key, ":", result[key]

REGISTER_WRITER(BasicWriter)


class IdentityMapper(BasicMapper):
    """IdentityMapper is a mapper that simply emits the same key value pair
    """

    def map(self, key, value):
        return key, value

REGISTER_MAPPER(IdentityMapper)


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
    """This reducer ignores all input keyvalue pairs.
    
    "You shall not pass!" - Gandalf the Grey
    """

    def reduce(self, key, values):
        pass

REGISTER_REDUCER(NoPassReducer)


