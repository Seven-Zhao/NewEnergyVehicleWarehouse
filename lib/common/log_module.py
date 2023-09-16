#!/usr/bin/python
# _*_ coding: utf-8 _*_
# @Time    : 2023/9/16 15:20
# @Author  : Seven
# @File    : log_module.py

import os
import logging
from logging import handlers

LOG_PATH = "/var/log/projects"


class Logger(object):
    log_level = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'crit': logging.CRITICAL
    }

    def __init__(self, project_name, file_name, level='info', when='D', back_count=3):
        fmt = '%(asctime)s-%(pathname)s[line:%(lineno)d]-%(levelname)s:%(message)s'
        self.logger = logging.getLogger(file_name)
        self.log_directory = '{}/{}'.format(LOG_PATH, project_name)
        self.log_filename = '{}/{}'.format(self.log_directory, file_name)
        format_str = logging.Formatter(fmt)
        # 设置日志级别
        self.logger.setLevel(self.log_level.get(level))

        # 创建日志目录
        os.makedirs(self.log_directory)

        # 屏幕输出的日志
        sh = logging.StreamHandler()
        # 屏幕日志的格式
        sh.setFormatter(format_str)

        # 往文件里面写入，指定间隔时间自动生成文件的处理器
        th = handlers.TimedRotatingFileHandler(filename=file_name, when=when, backupCount=back_count, encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)
