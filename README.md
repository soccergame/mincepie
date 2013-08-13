mincepie: Lightweight MapReduce in Python
============================================

Mincepie is a lightweight mapreduce engine purely written in Python for some of my research code. It is meant for simple distribution of jobs that:

- can be easily separated to map calls, such as extracting one feature for each image,
- does not involve a lot of communication, i.e. the keys and values being transmitted are not huge,
- the main computation time is spent inside the map() or reduce() function, not communication.

Also, the simplified system

- holds everything in memory - the input, the keys, and the values in every stage of the mapreduce run.
- does not handle server errors. If the server is down, you have to restart mapreduce.
- partially tolerates client failure. If a client is down, its last map() call will simply be re-run on another client.

I have used it to perform feature extractions on ImageNet and it works pretty well for our research use. But it may or may not fit your use case.

To see more documentation read [the wiki](https://github.com/Yangqing/mincepie/wiki).
