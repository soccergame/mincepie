"""
The mince module implements the server, the clients, and the communication
between the server and clients.

The communication part of this code is adapted from the original mincemeat 
code. The original license reads as follows:

*****
Copyright (c) 2010 Michael Fairley

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*****
 
Usually you don't need to import mince in your own mapreduce code - instead,
import mapreducer to write your mappers, reducers, readers and writers, and
import launcher to launch the mapreduce job.

Flags defined by this module:
    --password: a string for simple authentication between the server and the
        client. Default "default".
    --port: The port number for the mapreduce task.
    --address: The address of the server, if you are running the program as a 
        client manually.
    --timeout: the number of seconds before a client stops reconnecting to the 
        server, in case the server does not respond.
    --report_interval: the percentage interval between which we report the 
        progress of mapping. Default 10 (i.e. we report the elapsed time at
        10%, 20%, ...).

Modified by Yangqing Jia (jiayq@eecs.berkeley.edu)
"""

# python modules
import asynchat
import asyncore
import cPickle as pickle
import datetime
import gflags
import hashlib
import hmac
import logging
import os
import socket
import sys
import time

from . import mapreducer

# constant variables
SEPARATOR = ':'
TERMINATOR = '\n'
CONNECTION_WAIT_TIME = 1

# we use an enum to define the commands, just in case some typo takes place
# in coding.
class Enum(set):
    """A simple Enum class
    """
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError

COMMAND = Enum(['challenge',
                'auth',
                'disconnect',
                'map',
                'reduce',
                'mapdone',
                'reducedone',
               ])

TASK = Enum(['START',
             'MAPPING',
             'REDUCING',
             'FINISHED',
            ])


# flags defined for connection
gflags.DEFINE_string("password", "default",
    "The password for server client authentication")
gflags.DEFINE_integer("port", 11235, 
    "The port number for the mapreduce task")
gflags.DEFINE_string("address", "127.0.0.1",
    "The address of the server")
gflags.DEFINE_integer("timeout", 60,
    "The number of seconds before a client stops reconnecting")
gflags.DEFINE_integer("report_interval", 10,
    "The interval between which we report the elapsed time of mapping")

# FLAGS
FLAGS = gflags.FLAGS

class Protocol(asynchat.async_chat, object):
    """Communication protocol
    
    The Protocol class defines the basic protocol that both the server and
    the client will follow during the mapreduce execution.

    The basic function the protocol implements are:
        * provide parser for  the basic password and port for the connection
        * send command with possible arguments and data
        * deal with incoming data
        * Two-way authentication
    """
    def __init__(self, conn=None):
        if conn:
            asynchat.async_chat.__init__(self, conn)
        else:
            asynchat.async_chat.__init__(self)
        # We use the newline as the terminator
        self.set_terminator(TERMINATOR)
        self.buffer = []
        self.auth = None
        self.mid_command = None

    def collect_incoming_data(self, data):
        """Collect the incoming data and put it under buffer
        """
        self.buffer.append(data)

    def send_command(self, command, data=None, arg=None):
        """Send the command with optional data
        """
        encoded = command + SEPARATOR
        if arg:
            # this command contains some arguments
            encoded += arg
            #logging.debug("<- " + encoded)
            self.push(encoded + TERMINATOR)
        elif data:
            # this command contains pickled data
            pdata = pickle.dumps(data)
            encoded += str(len(pdata))
            #logging.debug("<- " + encoded + " (pickle)")
            self.push(encoded + TERMINATOR + pdata)
        else:
            #logging.debug("<- " + encoded)
            self.push(encoded + TERMINATOR)

    def decode_command(self, message):
        """decode the command to the command and the data
        """
        idx = message.find(SEPARATOR)
        if idx == -1:
            raise ValueError, "Unrecognized command: " + message
        return message[:idx], message[idx+1:]

    def found_terminator(self):
        message = "".join(self.buffer)
        if not self.auth == "Done":
            # before authentication, call process_unauthed_command
            command, data = self.decode_command(message)
            self.process_unauthed_command(command, data)
        elif self.mid_command is None:
            # if we are not in the middle of a command, we read the command
            # and process it, and also check if this command comes with a
            # data string
            command, length = self.decode_command(message)
            if command == COMMAND.challenge:
                # deal with challenge string
                #logging.debug("-> " + message)
                self.process_command(command, length)
            elif length:
                # this command comes with a data string: obtain the data
                #logging.debug("-> " + message + " (pickle)")
                self.set_terminator(int(length))
                self.mid_command = command
            else:
                # otherwise, simply process this command
                self.process_command(command)
        else:
            # Read the data segment from the previous command
            if not self.auth == "Done":
                logging.fatal("Recieved pickled data from unauthed source")
                sys.exit(1)
            data = pickle.loads(message)
            # reset the terminator and mid_command for the next command
            self.set_terminator(TERMINATOR)
            command = self.mid_command
            self.mid_command = None
            self.process_command(command, data)
        # clean the buffer
        self.buffer = []

    def send_challenge(self):
        self.auth = os.urandom(20).encode("hex")
        self.send_command(COMMAND.challenge, arg=self.auth)

    def respond_to_challenge(self, command, data):
        mac = hmac.new(FLAGS.password, data, hashlib.sha1)
        self.send_command(COMMAND.auth, arg=mac.digest().encode("hex"))
        self.post_auth_init()

    def verify_auth(self, command, data):
        mac = hmac.new(FLAGS.password, self.auth, hashlib.sha1)
        if data == mac.digest().encode("hex"):
            self.auth = "Done"
            logging.debug("Authenticated the other end")
        else:
            self.handle_close()

    def process_command(self, command, data=None):
        handlers = {
            COMMAND.challenge: self.respond_to_challenge,
            COMMAND.disconnect: lambda x,y: self.handle_close(),
            }
        if command in handlers:
            handlers[command](command, data)
        else:
            logging.critical("Unknown command received: " + command) 
            self.handle_close()

    def process_unauthed_command(self, command, data=None):
        handlers = {
            COMMAND.challenge: self.respond_to_challenge,
            COMMAND.auth: self.verify_auth,
            COMMAND.disconnect: lambda x, y: self.handle_close(),
            }
        if command in handlers:
            handlers[command](command, data)
        else:
            logging.critical("Unknown command received: " + command) 
            self.handle_close()
        

class Client(Protocol):
    def __init__(self):
        Protocol.__init__(self)
        self.mapper = None
        self.reducer = None

    def run_client(self, address = None):
        """Runs the client

        If address is None, the server address is obtaind from the commandline
        flags. Otherwise (e.g. we are running the whole mapreduce under MPI),
        the server address is the passed-in address.
        """
        if address is None:
            address = FLAGS.address
        logging.debug("Connecting to %s:%d" % (address, FLAGS.port))
        # connect, with possible failure
        time_spent = 0
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((address, FLAGS.port))
        while (not self.connected) and time_spent < FLAGS.timeout:
            try:
                self.connect((address, FLAGS.port))
            except socket.error, message:
                logging.debug("Conection failed, retry... " + str(message))
                # Calling __init__ is quite ugly, but there seems to be some 
                # rumor that asynchat has bugs dealing with socket error, and
                # this works now... Should investigate in the future.
                self.__init__()
                self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
                time.sleep(CONNECTION_WAIT_TIME)
                time_spent += CONNECTION_WAIT_TIME
        if self.connected:
            logging.debug('Connected!')
            asyncore.loop()
        else:
            self.close()

    def handle_connect(self):
        pass

    def handle_close(self):
        self.close()

    def call_map(self, command, data):
        """Calls the map function.
        
        Input:
            command: a dummy variable that equals to COMMAND.map
            data: a tuple containing the (key,value) pair
        """
        logging.debug("Mapping %s" % str(data[0]))
        results = {}
        if self.mapper is None:
            # create the mapper instance
            self.mapper = mapreducer.MAPPER(FLAGS.mapper)()
        for kvpair in self.mapper.map(data[0], data[1]):
            # if the mapper returns nothing, do nothing
            if kvpair is None:
                continue
            key, val = kvpair
            try:
                results[key].append(val)
            except KeyError:
                results[key] = [val]
        self.send_command(COMMAND.mapdone, (data[0], results))

    def call_reduce(self, command, data):
        """Calls the reduce function.

        See call_map for details
        """
        logging.debug("Reducing %s" % str(data[0]))
        if self.reducer is None:
            # create the reducer instance
            self.reducer = mapreducer.REDUCER(FLAGS.reducer)()
        results = self.reducer.reduce(data[0], data[1])
        self.send_command(COMMAND.reducedone, (data[0], results))
        
    def process_command(self, command, data=None):
        handlers = {
            COMMAND.map: self.call_map,
            COMMAND.reduce: self.call_reduce,
            }
        if command in handlers:
            handlers[command](command, data)
        else:
            # If key not recognized, fall back to the super class
            Protocol.process_command(self, command, data)

    def post_auth_init(self):
        if not self.auth:
            self.send_challenge()


class Server(asyncore.dispatcher, object):
    def __init__(self):
        asyncore.dispatcher.__init__(self)
        self._datasource = None
        self.taskmanager = None

    def set_datasource(self, datasource):
        self._datasource = datasource
        self.taskmanager = TaskManager(self._datasource, self)
    
    def get_datasource(self):
        return self._datasource

    datasource = property(get_datasource, set_datasource)

    def run_server(self):
        logging.info("Starting server.")
        self.datasource = mapreducer.READER(FLAGS.reader)().read(FLAGS.input)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        logging.info("Number of input key value pairs: %d " % \
                     (len(self.datasource.keys())))
        self.bind(("", FLAGS.port))
        self.listen(1)
        logging.info("Starting listening on %d" % (FLAGS.port))
        try:
            asyncore.loop()
        except:
            asyncore.close_all()
            raise
        logging.info("Mapreduce done.")
        mapreducer.WRITER(FLAGS.writer)().write(self.taskmanager.results)

    def handle_accept(self):
        pair = self.accept()
        if pair is None:
            pass
        conn, addr = pair
        logging.debug("New client arrived at " + str(addr))
        sc = ServerChannel(conn, addr, self)

    def handle_close(self):
        self.close()


class ServerChannel(Protocol):
    def __init__(self, conn, addr, server):
        Protocol.__init__(self, conn)
        self.server = server
        self.addr = str(addr)
        self.start_auth()

    def handle_close(self):
        logging.debug("Client %s disconnected" % (self.addr))
        self.close()

    def start_auth(self):
        self.send_challenge()

    def start_new_task(self):
        command, data = self.server.taskmanager.next_task(self)
        if command == None:
            return
        self.send_command(command, data)

    def map_done(self, command, data):
        self.server.taskmanager.map_done(data)
        self.start_new_task()

    def reduce_done(self, command, data):
        self.server.taskmanager.reduce_done(data)
        self.start_new_task()

    def process_command(self, command, data=None):
        handlers = {
            COMMAND.mapdone: self.map_done,
            COMMAND.reducedone: self.reduce_done,
            }
        if command in handlers:
            handlers[command](command, data)
        else:
            super(ServerChannel, self).process_command(command, data)

    def post_auth_init(self):
        self.start_new_task()
    

class TaskManager(object):
    def __init__(self, datasource, server):
        self.datasource = datasource
        self.num_maps = len(self.datasource.keys())
        self.num_done_maps = 0
        self.server = server
        self.state = TASK.START
        self.next_report_point = FLAGS.report_interval

    def next_task(self, channel):
        """Returns the next task to carry out
        """
        if self.state == TASK.START:
            logging.info("Start mapreduce.")
            self.map_iter = iter(self.datasource)
            self.working_maps = {}
            self.map_results = {}
            logging.info("Start map phase.")
            self.map_start_time = time.time()
            self.state = TASK.MAPPING
        
        if self.state == TASK.MAPPING:
            try:
                # get next map task
                map_key = self.map_iter.next()
                self.working_maps[map_key] = time.time()
                return (COMMAND.map, (map_key, self.datasource[map_key]))
            except StopIteration:
                # if we finished sending out all map tasks, select one task
                # from the existing pools (in case some of the jobs died for
                # some reason). If all maps are done, we go on to reduce
                if self.working_maps:
                    key = min(self.working_maps, key=self.working_maps.get)
                    self.working_maps[key] = time.time()
                    return (COMMAND.map, (key, self.datasource[key]))
                else:
                    logging.info("Map done. Start Reduce phase.")
                    self.state = TASK.REDUCING
                    self.reduce_iter = self.map_results.iteritems()
                    self.working_reduces = {}
                    self.results = {}

        if self.state == TASK.REDUCING:
            try:
                reduce_item = self.reduce_iter.next()
                self.working_reduces[reduce_item[0]] = time.time()
                return (COMMAND.reduce, reduce_item)
            except StopIteration:
                if self.working_reduces:
                    key = min(self.working_reduces,
                            key=self.working_reduces.get)
                    self.working_reduces[key] = time.time()
                    return (COMMAND.reduce, (key, self.map_results[key]))
                else:
                    logging.info("Reduce phase done.")
                    self.state = TASK.FINISHED
        if self.state == TASK.FINISHED:
            self.server.handle_close()
            return (COMMAND.disconnect, None)
    
    def map_done(self, data):
        # Don't use the results if they've already been counted
        if not data[0] in self.working_maps:
            return
        self.num_done_maps += 1 
        logging.debug('Map done (%d / %d): %s ' \
                      % (self.num_done_maps, self.num_maps, str(data[0])))
        # for logging.info, we only output the reports periodically
        ratio = int(self.num_done_maps * 100 / self.num_maps)
        if ratio >= self.next_report_point:
            elapsed = datetime.timedelta(
                    seconds=time.time() - self.map_start_time)
            logging.info("%d%% maps done. Elapsed %s." \
                    % (ratio, str(elapsed)))
            self.next_report_point += FLAGS.report_interval
        if data[1] is not None:
            for (key, values) in data[1].iteritems():
                if key not in self.map_results:
                    self.map_results[key] = []
                self.map_results[key].extend(values)
        del self.working_maps[data[0]]
                                
    def reduce_done(self, data):
        # Don't use the results if they've already been counted
        if not data[0] in self.working_reduces:
            return
        logging.debug('Reduce done: ' + repr(data[0]))
        if data[1] is not None:
            self.results[data[0]] = data[1]
        del self.working_reduces[data[0]]

if __name__ == "__main__":
    print __doc__

