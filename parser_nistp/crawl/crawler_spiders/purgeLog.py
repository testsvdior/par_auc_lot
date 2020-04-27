#!/usr/bin/python3

import shutil
import os
import sys

if(len(sys.argv) < 4:
    print("Missing arguments! Usage is script.py 9000 3")


file_name = sys.argv[1]
limitsize = int(sys.argv[2])
logsnumber = int(sys.argv[3])

if (os.path.isfile(file_name) == True:
    logfile_size = os.stat(file_name).st_size
    logfile_size = logfile_size // 1024

    if (logfile_size >= limitsize):
        if logsnumber > 0:
            for currentFileNum in range(logsnumber, 1, -1):
                scr = file_name + "_" + str(crrentFileNum-1)
                dst = file_name + "_" + str(currentFileNum)
                if os.path.isfile == True:
                    shutil.copyfile(src, dst)

            shutil.copyfile(file_name, file_name + "_1")
    myfile = open(file_name, "w")
    myfile.close()
