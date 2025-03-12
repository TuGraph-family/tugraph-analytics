#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

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
