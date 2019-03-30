# -*- coding: utf-8 -*-
#__Author__= allisnone #2019-02-16
#https://www.cnblogs.com/yyds/p/6901864.html
import logging
import logging.handlers
import datetime

def initial_logger(logfile='all.log',errorfile='error.log',logname='mylogger'):
    logger = logging.getLogger(logname)
    logger.setLevel(logging.DEBUG)
    
    rf_handler = logging.handlers.TimedRotatingFileHandler(logfile, when='midnight', interval=1, backupCount=7, atTime=datetime.time(0, 0, 0, 0))
    rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    f_handler = logging.FileHandler(errorfile)
    f_handler.setLevel(logging.ERROR)
    f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
    
    logger.addHandler(rf_handler)
    logger.addHandler(f_handler)
    
    logger.debug('debug message')
    logger.info('info message')
    logger.warning('warning message')
    logger.error('error message')
    logger.critical('critical message')
    return logger

#logger = initial_logger(logfile='all.log',errorfile='error.log',logname='mylogger')