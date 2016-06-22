#!/usr/bin/python

import sys, os, getopt, argparse, logging, logging.handlers, traceback, re, subprocess
import tweepy, json

## global variables
# logging configs
G_loglevel = logging.INFO
G_log_format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
G_log_maxbyte=10485760
G_log_backupcount=4
G_logger = None

# Authentication details. To  obtain these visit dev.twitter.com
G_consumer_key = '<consumer_key>'
G_consumer_secret = '<consumer_secret>'
G_access_token = '<access_token>'
G_access_token_secret = '<access_token_secret>'
G_track=['malaysia', 'msia', 'MALAYSIA', 'Malaysia']
G_locations=[99.64,1.27,104.53,6.72]


## preparing commandline argiments
def addArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("logfile", help="Logfile name [ex: /tmp/mylog.log]")
    args = parser.parse_args()
    return args


## kill same instance of the script (based on hivedatabase and hivetable)
def killSameInstalce(logfile):
    # accessing global variable
    global G_logger
    # killing duplicate prpcess
    G_logger.info("Killing script: "+os.path.basename(__file__)+" running with lofile:"+logfile)
    os.system("ps -ef --sort=start_time|grep \""+os.path.basename(__file__)+"\"|grep -i "+logfile+"|grep -iv \"bash oozie-oozi\"|grep -v grep|sed '$d'|awk '{print $2}'|xargs kill -9 >> "+logfile+" 2>&1 ")


## finction to format and print exception message
def printException(exc_info):
    exc_type, exc_value, exc_traceback = exc_info
    errorMessages = traceback.format_exception(exc_type, exc_value, exc_traceback)
    return ''.join('' + line for line in errorMessages)


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
    # redirect only strerror to logile, stdout should be only clean data
    #sys.stdout=StreamToLogger(logger, logging.INFO)
    sys.stderr=StreamToLogger(logger, logging.ERROR)

    return logger


## This is the listener, resposible for receiving data
class StdOutListener(tweepy.StreamListener):
    def on_data(self, data):
        # print to strout withput any filter to decoding
        print '%s' %  json.dumps(json.loads(data))  #str(json.load(data.strip())).replace("{u'","{'").replace(" u'"," '")
        return True

    def on_error(self, status):
        global G_logger
        G_logger.info(status)


## ____________main_________________
def main(argv):
    # accessing global variable
    global G_logger
    global G_consumer_key,  G_consumer_secret, G_access_token, G_access_token_secret, G_track, G_locations
    # setting arguments
    args = addArgs()
    # logging
    G_logger = getAppLogger(args.logfile)
    G_logger.info('Started loggong to '+args.logfile)
    G_logger.info("Start collection twitter data:_____________________________________________________________________")

    # Kill if there are any actives instance for the same hivedatabase & hivetable
    killSameInstalce(args.logfile)

    # twitter configs
    listener = StdOutListener()
    auth = tweepy.OAuthHandler(G_consumer_key, G_consumer_secret)
    auth.set_access_token(G_access_token, G_access_token_secret)

    stream = tweepy.Stream(auth, listener)
    stream.filter(track=G_track, locations=G_locations)


## callin main
if __name__ == '__main__':
    main(sys.argv)


