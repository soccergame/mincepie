"""Mincepie main

If you write your own mapreduce, you can use the default launcher to save some
time writing the main() function yourself. For example, if you have a module 
called mymr, you can run your script with
    (serverside) python -m mincepie --module=mymr --server [other arguments]
    (clientside) python -m mincepie --module=mymr [other arguments]
"""
from mincepie import launcher

if __name__ == "__main__":
    launcher.launch()
    