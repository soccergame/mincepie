The wordcount.py implements a basic example showing how to write mapper and
reducer functions, and launch the mapreduce server and clients.

To run the demo, on the server side, run:

python wordcount.py --server --input=*.txt

And on the client side, simply run

python wordcount.py

Optionally, you can add the following arguments to the server side so that
the counts are written to a file instead of dumped to stdout:

--writer=FileWriter --output=count.result

(count.result could be any filename)
