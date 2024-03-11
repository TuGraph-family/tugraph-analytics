import logging
import os
import time


def get_log(log_directory):
    logger = logging.getLogger()
    logger.setLevel(level=logging.INFO)
    time_line = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
    log_file_name = 'java-' + str(os.getppid()) + '-infer-process-' + str(os.getpid()) + '-' + time_line
    logfile = log_directory + '/' + log_file_name + '.txt'
    filer = logging.FileHandler(logfile, mode='w')
    file_formatter = logging.Formatter(
        fmt='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d  %H:%M:%S'
    )
    filer.setFormatter(file_formatter)
    logger.addHandler(filer)
    return logger
