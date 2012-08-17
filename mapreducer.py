# Basic mapreduce class, and a few default ones
#
# author: Yangqing Jia (jiayq84@gmail.com)


# The registerd mappers and reducers allow us to use strings to specify which
# mapper and reducer we want when running experiments
__registered_mappers = {}
__registered_reducers = {}
__registered_readers = {}

# RegisterMapper and RegisterReducerer methods
#
# These methods allow you to register your mapper and reducer.
def RegisterMapper(mapper):
    """Mapper Registerer

    Register your mapper so that the mapper string is recognizable as a
    string. For example, if you have your self-definedmapper class called
    FooMapper, call RegisterMapper(FooMapper). You will then able to get
    the mapper by its name using the Mapper() function like
        Mapper('FooMapper')
    This allows us to specify mapper using commandline arguments.
    """
    __registered_mappers[mapper.__name__] = mapper

def RegisterReducer(reducer):
    """Reducer Registerer

    See RegisterMapper() for details
    """
    __registered_reducers[reducer.__name__] = reducer

def RegisterReader(reader):
    """Reader Registerer

    See RegisterMapper() for details
    """
    __registered_readers[reader.__name__] = reader

# RegisterMapper and RegisterReducerer methods
#
# These methods returns the mapper or reducer class by its name as a string.
def Mapper(name):
    """Get Mapper by its name

    See RegisterMapper() for details
    """
    return __registered_mappers[name]

def Reducer(name):
    """Get Reducer by its name

    See RegisterMapper() for details
    """
    return __registered_reducers[name]

def Reader(name):
    """Get Reader by its name

    See RegisterMapper() for details
    """
    return __registered_readers[name]


class BasicMapper(object):
    """The basic mapper class. 
    
    All your mappers are belong to this.
    """

    def __init__(self):
        self.SetUp()

    def SetUp(self):
        """Any initialization should be written in the SetUp function.

        Note that no parameters are passed to SetUp(). For any paramter you
        need to initialize the mapper, use argparse.ArgumentParser, and then
        pass the parameters using commandline arguments.
        """
        pass

    def Map(self, k,v):
        """The map function for mapreduce.

        The input should be one key and one value. The output should be a list
        of (key, value) pairs, or a yield command that emits key value pairs.

        You should implement your own map function in your derived class.
        """
        raise NotImplementedError


class BasicReducer(object):
    """The basic reducer class. 
    
    All your reducerss are belong to this.
    """

    def __init__(self):
        self.SetUp()

    def SetUp(self):
        """Any initialization should be written in the SetUp function.

        Note that no parameters are passed to SetUp(). For any paramter you
        need to initialize the mapper, use argparse.ArgumentParser, and then
        pass the parameters using commandline arguments.
        """
        pass

    def Reduce(self, k, vs):
        """The reduce function for mapreduce.

        The input should be one key and one value. The output should be a list
        of (key, value) pairs, or a yield command that emits key value pairs.

        You should implement your own map function in your derived class.
        """
        raise NotImplementedError


class BasicReader(object):
    """The basic reader class

    All your readers are belong to this.
    """
    def __init__(self):
        self.SetUp()

    def SetUp(self):
        pass

    def Read(self, inputlist):
        raise NotImplementedError


class IdentityMapper(BasicMapper):
    """IdentityMapper is a mapper that simply emits the same key value pair
    """

    def Map(self, k,v):
        return k,v

RegisterMapper(IdentityMapper)


class SumReducer(BasicReducer):
    """SumReducer is a reducer that returns the sum of the values
    """
    
    def Reduce(self, k,vs):
        return sum(vs)

RegisterReducer(SumReducer)


class FirstElementReducer(BasicReducer):
    """FirstElementReducer is a reducer that takes the first value and ignores
    others
    """
    
    def Reduce(self, k, vs):
        return vs[0]

RegisterReducer(FirstElementReducer)


class NoPassReducer(BasicReducer):
    """This reducer ignores all input keyvalue pairs.
    
    "You shall not pass!" - Gandalf the Grey
    """

    def Reduce(self, k,vs):
        pass

RegisterReducer(NoPassReducer)


