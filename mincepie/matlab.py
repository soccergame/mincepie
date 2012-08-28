"""Simple Matlab mapper

This is a simple matlab mapper that you can use to deal with some legacy code
that still requires Matlab. It is not very fancy and complex - no Matlab 
engines are required for this, and all you need is the matlab command, as well
as python's subprocess module. 

The downside is that each map() function starts Matlab and closes it, which
causes additional overhead (several seconds, based on your Matlab config).
Thus, it should mostly be used to carry out computation-intensive map tasks
each lasting more than a few seconds. If your map() runs much shorter than
the Matlab start and finish overhead, it's probably not a good idea to
distribute your job, or you need to at least combine multiple small jobs in 
a larger map() call.
"""

from mincepie import mapreducer
from subprocess import Popen, PIPE
import logging

_CONFIG = {'matlab_bin': 'matlab',
           'args': ['-nodesktop','-nosplash','-nojvm','-singleCompThread']
          }
_SUCCESS_STR = '__mincepie.matlab.success__'
_FAIL_STR = '__mincepie.matlab.fail__'


def set_config(key, value):
    """Sets the config of matlab
    
    For example, you can set your own matlab bin:
    set_CONFIG('matlab_bin','/path/to/your/matlab/bin/matlab')
    """
    _CONFIG[key] = value


def wrap_command(command):
    """ We wrap the command in a try-catch pair. 
    
    If any exception is caught,
    we ask matlab to dump _FAIL_STR. Otherwise, matlab dumps _SUCCESS_STR.
    The returned string is then scanned by the mapper to determine the result
    of the mapreduce run
    """
    if type(command) is not list:
        command = [command]
    return ";\n".join(["try"] + command + [
        "fprintf(2,'%s')" % (_SUCCESS_STR),
        "catch ME",
        "disp(ME)",
        "disp(ME.message)",
        "disp(ME.stack)",
        "fprintf(2,'%s')" % (_FAIL_STR),
        "end",
        ])


class SimpleMatlabMapper(mapreducer.BasicMapper):
    """A simple Matlab Mapper that uses subprocess to run Matlab.
    
    To write your own Matlab mapper, make a derivative of SimpleMatlabMapper,
    and implement the make_command() function. Do NOT overwrite map() as you
    will usually do for python mappers.
    """
    # pylint: disable=R0201
    def make_command(self, key, value):
        """Make the Matlab command. You need to implement this in your code
        
        Example:
            def make_command(self, key, value):
                return ["fprintf('%s: %s\\n')" % (key, value)]
        """
        return ["fprintf('%s: %s\\n')" % (key, value)]

    def map(self, key, value):
        """ The map function of SimpleMatlabMapper and its derivatives.

        Do NOT override this with your own map() function - instead, write
        your own make_command(self, key, value) function.
        """
        command = wrap_command(self.make_command(key, value))
        try:
            proc = Popen([_CONFIG['matlab_bin']] + _CONFIG['args'],
                         stdin = PIPE, stdout = PIPE, stderr = PIPE)
        except OSError, errmsg:
            # if we catch OSError, we return the error for investigation
            logging.error(repr(errmsg))
            yield key, (False, errmsg, command)
        # pass the command to Matlab. 
        try:
            str_out, str_err = proc.communicate(command)
        # any exception inside the proc call will trigger the fail case.
        # pylint: disable=W0703
        except Exception, errmsg:
            # if proc.communicate encounters some error, return the error
            logging.error(repr(errmsg))
            yield key, (False, errmsg, command)
        # now, parse stderr to see whether we succeeded
        # pylint: disable=E1103
        if str_err.endswith(_SUCCESS_STR):
            yield key, (True, str_out, str_err)
        else:
            logging.error(str_out)
            logging.error(str_err)
            logging.error(command)
            yield key, (False, str_out, str_err, command)

mapreducer.REGISTER_MAPPER(SimpleMatlabMapper)

# Usually you shouldn't need a reducer for Matlab, but if you want, you can
# implement one as well.
mapreducer.REGISTER_DEFAULT_REDUCER(mapreducer.IdentityReducer)


if __name__ == "__main__":
    print __doc__
