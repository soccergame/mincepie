The wordcount.py implements a basic example showing how to write mapper and
reducer functions, and launch the mapreduce server and clients.

To run the demo, run:
    python wordcount.py --input=zen.txt
This will execute the server and the client both locally. Optionally, you can
run the server and client manually. On the server side, run:
    python wordcount.py --launch=server --input=zen.txt
And on the client side, simply run
    python wordcount.py --launch=client --address=SERVER_IP
where SERVER_IP is the ip or the hostname of the server. If you are running
locally, --address=SERVER_IP could be omitted.

Optionally, you can add the following arguments to the server side so that
the counts are written to a file instead of dumped to stdout:
    --writer=FileWriter --output=count.result
(count.result could be any filename)
