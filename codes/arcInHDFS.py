#!/usr/bin/python

import sys, os, sqlite3, getopt, argparse, logging, logging.handlers, traceback, re, subprocess, datetime

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
    parser.add_argument("-ap", "--archivepath", help="Absolute HDFS directory location where to archive files.")
    args = parser.parse_args()
    return args


## finction to format and print exception message
def printException(exc_info):
    exc_type, exc_value, exc_traceback = exc_info
    errorMessages = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return ''.join('' + line for line in errorMessages)


## kill same instance of the script (based on sourcepath and destinationpath)
def killSameInstalce(sourcepath, archivepath, logfile):
    # accessing global variable
    global G_logger
    
    # killing duplicate prpcess
    G_logger.info("Killing script: "+os.path.basename(__file__)+" running for source:"+sourcepath+" and archivepath:"+archivepath)
    os.system("ps -ef --sort=start_time|grep \""+os.path.basename(__file__)+"\"|grep -i "+sourcepath+"|grep -i "+archivepath+"|grep -iv \"bash oozie-oozi\"|grep -v grep|sed '$d'|awk '{print $2}'|xargs kill -9 >> "+logfile+" 2>&1")


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

    if args.archivepath:
        G_logger.debug("FUNCTION:parseArgs:Input ARCHIVEPATH: "+ args.archivepath)
    else:
        G_logger.error("-ap/--archivepath not found.")
        exit(1)

    return args.sourcepath, args.archivepath


## function for calling OS hdfs command move files from source HDFS directory to destination HDFS directory 
def OsHDFSArc(sourcepath, archivepath, logfile):
    # accessing global variable
    global G_logger
  
    # Creating today's arc directory 
    curr_arc_dir=archivepath+"/"+datetime.datetime.now().strftime("%Y%m%d") 
    G_logger.info("Creating today's arc directory "+curr_arc_dir)
    os.system("hadoop fs -mkdir -p "+curr_arc_dir+" >> "+logfile+" 2>&1 ") 
   
    # start moving files in to HDFS archive 
    G_logger.info("Start moving files to destination HDFS archive path:"+curr_arc_dir)
    os.system("hadoop fs -mv "+sourcepath+"/* "+curr_arc_dir +" >> "+logfile+" 2>&1")
    G_logger.info("Complete moving files to destination HDFS archive path:"+curr_arc_dir)


##____________________main________________________

def main(argv):
    # accessing global variable
    global G_logger

    # setting arguments
    args = addArgs()

    # logging
    G_logger = getAppLogger(args.logfile)
    print 'Started loggong to '+args.logfile
    G_logger.info("\n\n")
    G_logger.info("Start of hdfs archive process:_____________________________________________________________________")

    # collect all the arguments values
    sourcepath, archivepath = parseArgs(args)
    G_logger.info("Argument values:\n SOURCEPATH:"+sourcepath+"\n ARCHIVEPATH:"+archivepath)

    # Kill if there are any actives instance for the same sourcepath & hdfspath
    killSameInstalce(sourcepath, archivepath, args.logfile)

    # mv files from source HDFS dir to destination HDFS arc dir
    OsHDFSArc(sourcepath, archivepath, args.logfile)

    G_logger.info("End of hdfs archive process:_______________________________________________________________________\n\n")


if __name__ == "__main__":
    main(sys.argv)






