"""Common mincepie launchers

This module provides some off-the-shelf launcher that you can use to simplify
your job scheduling. There are different types of launch mode you can specify -
refer to the documentation for details about each launch mode.

Flags defined by this module:
    --loglevel: the level for logging output. 20 for logging.INFO and 10 for
        logging.DEBUG. Refer to the logging module for more details.
    --launch: the launch mode. can be "local" (default), "server", "client", 
        "mpi", or "slurm".
    --num_clients: the number of clients. Only used when the launch mode is 
        local or slurm (in which this number of slurm jobs are submitted, 
        although the actual number of running clients are also constrained by
        the slurm resource limit). Default 1.

Slurm-specific flags:
    --slurm_shebang: the shebang used to create the slurm command. Default 
        "#!/bin/bash".
    --slurm_python_bin: the command to call python in the client. Default
        "python".
    --sbatch_bin: the command to call sbatch. Default "sbatch".
    --scancel_bin: the command to call scancel. Default "scancel".
    --sbatch_args: the sbatch arguments. Can be specified multiple times to add
        multiple arguments, such as
            --sbatch_args=--mem=1000 --sbatch_args=--partition=default
        See the sbatch man page for the list of sbatch arguments.

Yangqing jia, jiayq@eecs.berkeley.edu
"""

# python modules
import gflags
import hashlib
import logging
from mincepie import mince
from multiprocessing import Process
import socket
from subprocess import Popen, PIPE
import sys
import time

gflags.DEFINE_integer("loglevel", 20,
        "The level for logging. 20 for INFO and 10 for DEBUG.")
gflags.DEFINE_string("launch", "local",
        "The launch mode. See mincepie.launcher.launch() for details.")
gflags.DEFINE_integer("num_clients", 1,
        "The number of clients. Does not apply in the case of MPI.")
gflags.RegisterValidator('num_clients', lambda x: x > 0,
                         message='--num_clients must be positive.')
# slurm flags
gflags.DEFINE_string("slurm_shebang", "#!/bin/bash",
        "The shebang of the slurm batch script")
gflags.DEFINE_string("slurm_python_bin", "python",
        "The command to call python")
gflags.DEFINE_string("sbatch_bin", "sbatch",
        "The command to call sbatch")
gflags.DEFINE_string("scancel_bin", "scancel",
        "The command to call scancel")
gflags.DEFINE_multistring("sbatch_args", [],
        "The sbatch arguments")
# easy access to FLAGS
FLAGS = gflags.FLAGS


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
        launch_local()
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
    elif FLAGS.launch == "slurm":
        launch_slurm(argv)
    else:
        logging.fatal("Unable to recognize the launch mode: "+ FLAGS.launch)
        sys.exit(1)
    logging.info("Mapreduce terminated.")
    return

def launch_local():
    """ launches both the server and the clients on the local machine.
    
    The number of clients is defined in FLAGS.num_clients.
    """
    server, client = mince.Server(), mince.Client()
    serverprocess = Process(target = server.run_server, args = ())
    serverprocess.start()
    clientprocess = []
    for i in range(FLAGS.num_clients):
        clientprocess.append(
                Process(target = client.run_client, args = ()))
        clientprocess[i].start()
    serverprocess.join()
    logging.info("Waiting for processes to finish.")
    # some clients might still be running, and we will wait for them
    for i in range(FLAGS.num_clients): 
        clientprocess[i].join()
    return

def launch_slurm(argv):
    """ launches the server on the local machine, and sbatch slurm clients

    Commandline arguments:
        --num_clients
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
    if (FLAGS.num_clients <= 0):
        logging.fatal("The number of slurm clients should be positive.")
        sys.exit(1)
    # first, run server
    server = mince.Server()
    serverprocess = Process(target = server.run_server, args=())
    serverprocess.start()
    # now, submit slurm jobs
    logging.info('Submitting slurm jobs.')
    logging.info('Command:\n'+command)
    with open(jobname + '.sh', 'w') as fid:
        fid.write(command)
        fid.close()
    logging.info('Command saved to %s.sh', jobname)
    logging.info('Use sbatch %s.sh to add jobs if you want.', jobname)
    for i in range(FLAGS.num_clients):
        args = [FLAGS.sbatch_bin, '--job-name=%s' % (jobname,)]
        if len(FLAGS.sbatch_args) > 0:
            args += FLAGS.sbatch_args
        proc = Popen(args, stdin = PIPE, stdout = PIPE, stderr = PIPE)
        out, err = proc.communicate(command)
        if err != "":
            # sbatch seem to have returned an error
            logging.fatal("Sbatch does not run as expected.")
            logging.fatal("Stdout:\n" + out)
            logging.fatal("Stderr:\n" + err)
            serverprocess.terminate()
            sys.exit(1)
        else:
            logging.debug("Slurm job #%d: %s", i, str(out).strip())
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

if __name__ == "__main__":
    print __doc__
