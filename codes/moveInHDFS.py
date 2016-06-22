#!/usr/bin/python

import sys, os, sqlite3, getopt, argparse, logging, logging.handlers, traceback, re, subprocess

## global variables
G_loglevel = logging.INFO
G_log_format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
G_log_maxbyte=10485760
G_log_backupcount=4
G_logger = None

## preparing commandline argiments
def addArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("logfile", help="Logfile name [ex: /tmp/mylog.log]")
    parser.add_argument("-sp", "--sourcepath", help="Absolute HDFS directory location with target files.")
    parser.add_argument("-dp", "--destinationpath", help="Absolute HDFS directory location where to move files.")
    parser.add_argument("-n", "--numberoffiles", help="Number of files to be copied")
    parser.add_argument("-f", "--filename", help="RegEx of target files [example: for files starts with 'users' and ends with '.gz', regex will be '^users.*\.gz$'")
    args = parser.parse_args()
    return args


## finction to format and print exception message
def printException(exc_info):
    exc_type, exc_value, exc_traceback = exc_info
    errorMessages = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return ''.join('' + line for line in errorMessages)


## kill same instance of the script (based on sourcepath and destinationpath)
def killSameInstalce(sourcepath, destinationpath, logfile):
    # accessing global variable
    global G_logger
    
    # killing duplicate prpcess
    G_logger.info("Killing script: "+os.path.basename(__file__)+" running for source:"+sourcepath+" and destination:"+destinationpath)
    os.system("ps -ef --sort=start_time|grep \""+os.path.basename(__file__)+"\"|grep -i "+sourcepath+"|grep -i "+destinationpath+"|grep -iv \"bash oozie-oozi\"|grep -v grep|sed '$d'|awk '{print $2}'|xargs kill -9 >> "+logfile+" 2>&1 ")


## straem logger class to enable stdout and stderr logging
class StreamToLogger(object):
   """
   Fake file-like stream object that redirects writes to a logger instance.
   """
   def __init__(self, logger, log_level=logging.INFO):
      self.logger = logger
      self.log_level = log_level
      self.linebuf = ''

   def write(self, buf):
      for line in buf.rstrip().splitlines():
         self.logger.log(self.log_level, line.rstrip())


## logfile setting
def getAppLogger(logfile):
    # accessing global variable
    global G_loglevel, G_log_format, G_log_maxbyte, G_log_backupcount

    # configure log formatter
    logFormatter = logging.Formatter(G_log_format)

    # configuring rolation handler and logfile
    rotateHandler=logging.handlers.RotatingFileHandler(logfile, mode='a', maxBytes=G_log_maxbyte, backupCount=G_log_backupcount)
    rotateHandler.setFormatter(logFormatter)

    # get the logger instance
    logger = logging.getLogger(__name__)

    # set the logging level
    logger.setLevel(G_loglevel)

    # adding handlers
    logger.addHandler(rotateHandler)
    # redirect strout and strerr to logger
    sys.stdout=StreamToLogger(logger, logging.INFO)
    sys.stderr=StreamToLogger(logger, logging.ERROR)

    return logger


def parseArgs(args):
    # accessing global variable
    global G_logger

    if args.sourcepath:
        G_logger.debug("FUNCTION:parseArgs:Input SOURCEPATH: "+ args.sourcepath)
    else:
        G_logger.error("-sp/--sourcepath not found.")
        exit(1)

    if args.destinationpath:
        G_logger.debug("FUNCTION:parseArgs:Input DESTINATIONPATH: "+ args.destinationpath)
    else:
        G_logger.error("-dp/--destinationpath not found.")
        exit(1)

    if args.numberoffiles:
        G_logger.debug("FUNCTION:parseArgs:Input NUMBEROFFILES: "+ args.numberoffiles)
    else:
        G_logger.error("-n/--numberoffiles not found.")
        exit(1)

    if args.filename:
        G_logger.debug("FUNCTION:parseArgs:Input FILENAME: "+ args.filename)
    else:
        G_logger.error("-f/--filename not found.")
        exit(1)

    return args.sourcepath, args.destinationpath, args.numberoffiles, args.filename


## list files from HDFS directory
def listHDFS(sourcepath, numberoffiles, filename):
    # accessing global variable
    global G_logger

    G_logger.debug("listHDFS: collect ls output on HDFS directory")
    # collect ls output on HDFS directory
    p1 = subprocess.Popen(["hadoop", "fs", "-ls", sourcepath], stdout=subprocess.PIPE)

    output = p1.communicate()[0]
    # split the output as list entries
    files_list=output.split("\n")

    files = []
    idx = 0
    for element in files_list:
        if idx <= int(numberoffiles):        
            idx = idx + 1
            line_list = element.split()
            if len(line_list) > 6: # as in hadoop output 8th position is the filename
                if re.search(filename, os.path.basename(line_list[7])): # checking whthere the name is of desired format
                    files.append(os.path.basename(line_list[7])) # collectoing only filename, not full path
    
    # remove any empty string elements
    files = [file for file in files if file]

    return files  
   

## function for calling OS hdfs command move files from source HDFS directory to destination HDFS directory 
def OsHDFSMove(sourcepath, destinationpath, numberoffiles, filename, logfile):
    # accessing global variable
    global G_logger
   
    # Collect filenames in local source directory 
    G_logger.info("Collect filenames in source HDFS directory:"+sourcepath)
    files = listHDFS(sourcepath, numberoffiles, filename)

    # start moving files in HDFS
    G_logger.info("Start moving files to destination HDFS path:"+destinationpath)
    for f in files:
        G_logger.info("Start transfering file: "+f+" to "+destinationpath)
        os.system("hadoop fs -mv "+sourcepath+"/"+f+" "+destinationpath+" >> "+logfile+" 2>&1")
        G_logger.info("Complete moving file: "+f+" to "+destinationpath)


##____________________main________________________

def main(argv):
    # accessing global variable
    global G_logger

    # setting arguments
    args = addArgs()

    # logging
    G_logger = getAppLogger(args.logfile)

    print 'Started loggong to '+args.logfile
    G_logger.info("Start of hdfs move process:_____________________________________________________________________")

    # collect all the arguments values
    sourcepath, destinationpath, numberoffiles, filename = parseArgs(args)
    G_logger.info("Argument values:\n SOURCEPATH:"+sourcepath+"\n DESTINATIONPATH:"+destinationpath+"\n NUMBEROFFILES:"+numberoffiles+"\n FILENAME:"+filename)

    # Kill if there are any actives instance for the same sourcepath & hdfspath
    killSameInstalce(sourcepath, destinationpath, args.logfile)

    # mv files from source HDFS dir to destination HDFS dir
    OsHDFSMove(sourcepath, destinationpath, numberoffiles, filename, args.logfile)

    G_logger.info("End of hdfs move process:_______________________________________________________________________\n\n")


if __name__ == "__main__":
    main(sys.argv)






