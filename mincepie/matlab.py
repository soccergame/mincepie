"""
Simple Matlab mapper for mincepie.

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

Flags defined by this module:
    --singlethread: set matlab to use single thread only.

Yangqing jia, jiayq@eecs.berkeley.edu
"""

import gflags
import logging
from mincepie import mapreducer
from subprocess import Popen, PIPE

gflags.DEFINE_bool('singlethread', False, \
        'If set, the matlab will run with single thread.')
FLAGS = gflags.FLAGS

_CONFIG = {'matlab_bin': 'matlab',
           'args': ['-nodesktop','-nosplash','-nojvm']
          }
_SUCCESS_STR = '__mincepie.matlab.success__'
_FAIL_STR = '__mincepie.matlab.fail__'

def _set_singlecompthread():
    """Set the matlab command to use single thread only.

    Note that in default, this module will launch Matlab that uses all the cores
    available on a machine.
    """
    if '-singleCompThread' not in _CONFIG['args']:
        _CONFIG['args'].append('-singleCompThread')

def set_config(key, value):
    """Sets the configurations of matlab.
    
    For now, , you can set the following configs:
        - Your own matlab bin:
            set_config('matlab_bin','/path/to/your/matlab/bin/matlab')
        - Your own matlab launch args:
            set_config('args', ['-nodesktop','-nosplash','-nojvm'])
    """
    _CONFIG[key] = value

def get_config(toprint = False):
    """Prints the current configuration of Matlab.

    Input:
        toprint: If True, also print the configuration. Default False.
    Output:
        config: a python dictionary specifying the current configuration. Note
            that changing the values in the returned config won't affect the
            actual configuration - use set_config() instead.
    """
    if toprint is not False:
        for key in _CONFIG:
            print '%s: %s' % (key, repr(_CONFIG[key]))
    return dict(_CONFIG)

def _wrap_command(command):
    """Wrap the command in a try-catch pair. 
    
    If any exception is caught, we ask matlab to dump _FAIL_STR. Otherwise, 
    matlab dumps _SUCCESS_STR. The returned string could then be scanned by the
    mapper to determine the result of the mapreduce run.

    Input:
        command: a list of Matlab commands. Even if the commands do not end with
            semicolons, we will add them to each command, so explicitly use
            fprintf or disp if you want to display something.
    """
    if type(command) is not list:
        command = [command]
    return ";\n".join(
            ["try"] + \
            command + \
            ["fprintf(2,'%s')" % (_SUCCESS_STR),
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
        """Make the Matlab command. You need to implement this in your code.
        
        Example:
            def make_command(self, key, value):
                return ["fprintf('%s: %s\\n')" % (key, value)]
        """
        return ["fprintf('%s: %s\\n')" % (key, value)]

    def map(self, key, value):
        """ The map function of SimpleMatlabMapper and its derivatives.

        Do NOT override this with your own map() function - instead, write
        your own make_command(self, key, value) function.

        Input:
            key, value: the input key and value in the default mapreduce
                setting.
        Yield:
            key: the same key as the input key
            value: a tuple containing the running result of Matlab. There are
                several cases:
                (1) Matlab failed to launch or execute correctly as a subprocess
                    and a python exception is caught. In this case, the returned
                    tuple will be (False, errmsg, command), where errmsg is the
                    python exception and command is the command passed to
                    Matlab.
                (2) Matlab successfully finished, but the Matlab code created
                    an error inside Matlab. In this case, the returned tuple
                    will be (False, stdout, stderr, command), where stdout and
                    stderr are the output from the Matlab program.
                (3) Matlab successfully finished running. In this case, the
                    returned tuple will be (True, stdout, stderr, command).
        """
        if FLAGS.singlethread:
            _set_singlecompthread()
        # obtain the Matlab command first
        command = _wrap_command(self.make_command(key, value))
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
# implement one as well. We set the default reducer to IdentityReducer.
mapreducer.REGISTER_DEFAULT_REDUCER(mapreducer.IdentityReducer)

if __name__ == "__main__":
    print __doc__
