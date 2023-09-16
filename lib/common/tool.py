#!/usr/bin/python
# _*_ coding: utf-8 _*_
# @Time    : 2023/9/16 23:15
# @Author  : Seven
# @File    : tool.py
import os
import time
import zipfile
from pathlib import Path


def get_linux_style_path(src_path, *args):
    dst_path = src_path
    for tmp in args:
        dst_path = os.path.join(dst_path, tmp)
    dst_path = Path(dst_path).as_posix()
    return dst_path


def get_local_time():
    time_str = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
    return time_str


def zip_file(source_dir, target):
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(source_dir, arcname=(os.path.basename(source_dir)))
        for root, dirs, files in os.walk(source_dir):
            for fn in files:
                myzip.write(os.path.join(root, fn),
                            arcname=os.path.basename(source_dir) + '/' + os.path.relpath(os.path.join(root, fn),
                                                                                         source_dir))


def remove_file(file_path):
    try:
        os.remove(file_path)
    except Exception as e:
        print(e.__context__)


def mkdir_file(file_path):
    try:
        if not os.path.exists(file_path):
            os.makedirs(file_path)
    except Exception as e:
        print(e.__context__)
