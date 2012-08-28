"""Common mincepie launchers

This module provides some off-the-shelf launcher that you can use to simplify
your job scheduling.

author: Yangqing Jia (jiayq84@gmail.com)
"""

# python modules
import gflags
import hashlib
import logging
from mincepie import mince
from multiprocessing import Process
import time
import socket
from subprocess import Popen, PIPE
import sys

gflags.DEFINE_integer("loglevel", 20,
        "The level for logging. 20 for INFO and 10 for DEBUG.")
gflags.DEFINE_string("launch", "local",
        "The launch mode. See mincepie.launcher.launch() for details.")
# slurm flags
gflags.DEFINE_integer("slurm_numjobs", 0,
        "The number of clients to be submitted to slurm")
gflags.DEFINE_string("slurm_shebang", "#!/bin/bash",
        "The shebang of the slurm batch script")
gflags.DEFINE_string("slurm_python_bin", "python",
        "The command to call python")
gflags.DEFINE_string("sbatch_bin", "sbatch",
        "The command to call sbatch")
gflags.DEFINE_string("scancel_bin", "scancel",
        "The command to call scancel")
gflags.DEFINE_string("sbatch_args", "",
        "The sbatch arguments")
FLAGS = gflags.FLAGS
_SLURM_WARNING_LIMIT = 100

def process_argv(argv):
    """processes the arguments using gflags
    """
    try:
        # parse flags
        inputlist = gflags.FLAGS(argv)
    except gflags.FlagsError, message:
        print '%s\\nUsage: %s ARGS\\n%s' % (message, argv[0], gflags.FLAGS)
        sys.exit(1)
    # set some common stuff
    logging.basicConfig(level=FLAGS.loglevel)
    return inputlist[1:]


def launch(argv=None):
    """Launches the program with commandline flag
    """
    if argv is None:
        argv = sys.argv
    process_argv(argv)
    if FLAGS.launch == 'local':
        server, client = mince.Server(), mince.Client()
        serverprocess = Process(target = server.run_server, args = ())
        clientprocess = Process(target = client.run_client, args = ())
        serverprocess.start()
        clientprocess.start()
        clientprocess.join()
        serverprocess.join()
    elif FLAGS.launch == "server":
        # server mode
        server = mince.Server()
        server.run_server()
    elif FLAGS.launch == "client":
        # client mode
        client = mince.Client()
        client.run_client()
    elif FLAGS.launch == "mpi":
        launch_mpi()
    elif FLAGS.launch == 'slurm':
        launch_slurm(argv)
    return


def launch_slurm(argv):
    """ launches the server on the local machine, and sbatch slurm clients

    Commandline arguments:
        --slurm_numjobs
        --sbatch_bin
        --sbatch_args
        --slurm_shebang
        --slurm_python_bin
    """
    address = socket.gethostbyname(socket.gethostname())
    command = "%s\n%s %s --address=%s --launch=client" \
                % (FLAGS.slurm_shebang,
                   FLAGS.slurm_python_bin,
                   " ".join(argv),
                   address)
    jobname = hashlib.md5(argv[0] + str(FLAGS.port) + str(time.time()))\
                     .hexdigest()
    if (FLAGS.slurm_numjobs <= 0):
        logging.fatal("The number of slurm clients should be positive.")
        sys.exit(1)
    if (FLAGS.slurm_numjobs > _SLURM_WARNING_LIMIT):
        logging.warning("Really? So many slurm clients?")
    # first, run server
    server = mince.Server()
    serverprocess = Process(target = server.run_server, args=())
    serverprocess.start()
    # now, submit slurm jobs
    logging.info('Submitting slurm jobs.')
    logging.info('Command:\n'+command)
    for i in range(FLAGS.slurm_numjobs):
        args = [FLAGS.sbatch_bin, '--job-name=%s' % (jobname,)]
        if FLAGS.sbatch_args != "":
            args += FLAGS.sbatch_args.split(" ")
        proc = Popen(args, stdin = PIPE, stdout = PIPE, stderr = PIPE)
        out, err = proc.communicate(command)
        if err != "":
            # sbatch seem to have returned an error
            logging.fatal("Sbatch does not run as expected.")
            logging.fatal("Stdout:\n" + out)
            logging.fatal("Stderr:\n" + err)
            sys.exit(1)
        else:
            logging.debug("Slurm job #%d: " % (i) + out.strip())
    # wait for server process to finish
    serverprocess.join()
    logging.debug("Removing any pending jobs.")
    proc = Popen([FLAGS.scancel_bin, '--name=%s' % (jobname,)],
                stdin = PIPE, stdout = PIPE, stderr = PIPE)
    # Here we simply do a communicate and discard the results
    # We may want to handle the case when scancel fails?
    proc.communicate()
    return

def launch_mpi():
    """Launches the program with MPI

    The mpi root host runs in server mode, and others run in client mode.
    Note that you need to have more than 1 mpi host for this to work.
    """
    try:
        from mpi4py import MPI
    except ImportError:
        logging.fatal('To use launch_mpi, you need mpi4py installed.')
        sys.exit(1)
    comm = MPI.COMM_WORLD
    if comm.Get_size() == 1:
        logging.error('You need to specify more than one MPI host.')
        sys.exit(1)
    # get the server address
    address = socket.gethostbyname(socket.gethostname())
    address = comm.bcast(address)
    if comm.Get_rank() == 0:
        # server mode
        server = mince.Server()
        server.run_server()
        # after the server finishes running, tere might be
        # some clients still running, and MPI does not exit very elegantly. 
        # However, with asynchat and the current implementation we have no 
        # trace of running clients, so this is probably inevitable.
    else:
        # client mode
        client = mince.Client()
        client.run_client(address)
    return

