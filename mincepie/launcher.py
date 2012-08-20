"""Common mincepie launchers

This module provides some off-the-shelf launcher that you can use to simplify
your job scheduling.

author: Yangqing Jia (jiayq84@gmail.com)
"""

# python modules
import gflags
from mincepie import mince
import socket
import sys


def process_argv(argv):
    """processes the arguments using gflags
    """
    if argv is None:
        argv = sys.argv
    try:
        # parse flags
        inputlist = gflags.FLAGS(argv)
        return inputlist[1:]
    except gflags.FlagsError, message:
        print '%s\\nUsage: %s ARGS\\n%s' % (message, argv[0], gflags.FLAGS)
        sys.exit(1)


def launch(argv=None):
    """Launches the program with commandline flag

    If --server is set, run in server mode; otherwise, run in client mode
    """
    inputlist = process_argv(argv)
    if gflags.FLAGS.server:
        # server mode
        server = mince.Server()
        server.run_server(inputlist)
    else:
        # client mode
        client = mince.Client()
        client.run_client()
    return


def launch_mpi(argv = None):
    """Launches the program with MPI

    The mpi root host runs in server mode, and others run in client mode.
    Note that you need to have more than 1 mpi host for this to work.
    """
    try:
        from mpi4py import MPI
    except ImportError:
        print 'To use launch_mpi, you need mpi4py installed.'
        sys.exit(1)
    comm = MPI.COMM_WORLD
    if comm.Get_size() == 1:
        print 'You need to specify more than one MPI host.'
        sys.exit(1)
    inputlist = process_argv(argv)
    # get the server address
    address = socket.gethostbyname(socket.gethostname())
    address = comm.bcast(address)
    if comm.Get_rank() == 0:
        # server mode
        server = mince.Server()
        server.run_server(inputlist)
        # after the server finishes running, tere might be
        # some clients still running, and MPI does not exit very elegantly. 
        # However, with asynchat and the current implementation we have no 
        # trace of running clients, so this is probably inevitable.
    else:
        # client mode
        client = mince.Client()
        client.run_client(address)
    return

