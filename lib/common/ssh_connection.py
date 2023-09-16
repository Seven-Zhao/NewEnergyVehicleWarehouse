#!/usr/bin/python
# _*_ coding: utf-8 _*_
# @Time    : 2023/9/16 21:58
# @Author  : Seven
# @File    : ssh_connection.py
import os.path
import time
import traceback

import paramiko


class LongSSHConnection(object):
    PS1_DEFAULT = u"]$ "

    def __init__(self, node, ps1=PS1_DEFAULT, pkey_file=""):
        self.username = node.username
        self.password = node.password
        self.ip = node.ip
        self.port = node.port
        self.ps1 = ps1 if isinstance(ps1, str) else self.PS1_DEFAULT
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.chan = None
        self._open_channel(pkey_file=pkey_file)
        self.sftp = self.ssh.open_sftp()

    def _open_channel(self, timeout=60, pkey_file=""):
        timeout = int(timeout)
        try_times = 1
        max_try_times = 3
        while try_times <= max_try_times:
            try:
                result = self._open_channel_once(timeout, pkey_file)
                if result:
                    return
            except Exception as e:
                traceback.print_exc()
                time.sleep(2)
            finally:
                try_times += 1
        raise Exception("ssh connect to host {} timeout".format(self.ip))

    def _open_channel_once(self, timeout=60, pkey_file=""):
        if pkey_file == "":
            self.ssh.connect(self.ip, self.port, username=self.username, password=self.password, timeout=timeout,
                             look_for_keys=False)
        else:
            private_key = paramiko.RSAKey.from_private_key_file(pkey_file)
            self.ssh.connect(self.ip, self.port, self.username, pkey=private_key, timeout=timeout)
        self.ssh.get_transport().set_keepalive(30)
        self.chan = self.ssh.invoke_shell("dumb", 500, 500)

        start_time = time.time()
        current_time = time.time()
        has_set_var = False

        while current_time - start_time < timeout:
            if not has_set_var and self.chan.send_ready():
                self.chan.send('TMOUT=0;PS1=\'%s\';echo ----"$PS1"----\n'.encode('utf-8') % self.ps1.encode('utf-8'))
                has_set_var = True

            if self.chan.recv_ready():
                info = self.chan.recv(65535)
                if "----%s----".encode('utf-8') % self.ps1.encode('utf-8') in info:
                    break
            current_time = time.time()
            if current_time - start_time >= timeout:
                self.close()
                raise IOError("wait for ssh send ready to {} timeout.".format(self.ip))
            time.sleep(1)
        return True

    def execute_cmd(self, cmd, expect_end=(PS1_DEFAULT.strip(),), timeout=30, channel=None):
        try:
            return self._execute_cmd(cmd, expect_end, timeout, channel)
        except Exception as e:
            traceback.print_exc()
            return ""

    def _execute_cmd(self, cmd, expect_end=(PS1_DEFAULT.strip(),), timeout=30, channel=None):
        if isinstance(expect_end, tuple):
            _expect_end = tuple((_item.strip() for _item in expect_end))
        else:
            _expect_end = expect_end.strip()

        chan = channel if channel else self.chan
        timeout = int(timeout)
        result = ''
        start_time = time.time()

        while chan.recv_ready():
            tmp_out = chan.recv(65535)
            time.sleep(0.5)

        _data = cmd + '\n'
        _have_sent = 0
        while time.time() - start_time <= timeout:
            if chan.send_ready():
                _send_bytes = chan.send(_data[_have_sent:])
                _have_sent += _send_bytes
                if _have_sent == len(_data):
                    break
            time.sleep(0.1)
        if len(_data) == _have_sent:
            print(f"CommandLength={len(_data)}, SentLength={_have_sent}")
        else:
            raise Exception("send error")

        while True:
            time.sleep(0.5)
            if chan.recv_ready():
                ret = chan.recv(65535)
                if "client_password" in ret.decode() or "key" in ret.decode() or "users.dat" in ret.decode():
                    print(f"On node({self.ip}): message")
                else:
                    print(f"On node({self.ip}):{ret.decode()}")
                result += ret.decode()
            current_time = time.time()
            if result.strip().endswith(_expect_end) or current_time - start_time >= timeout:
                start_index = 0
                if result.startswith(cmd):
                    start_index = len(cmd)
                if current_time - start_time >= timeout:
                    print(f"On node({self.ip}:receive timeout({timeout}) "
                          f"and return the temporary output:{result.strip()[start_index:]}")
                return result.strip()[start_index:]

    def put_file(self, src_file_path, dst_path, dst_type="DIR", retry_limit=3, new_sftp=None):
        if dst_type.lower() == 'dir':
            dst_file_path = os.path.join(dst_path, os.path.basename(src_file_path)).replace(os.sep, '/')
        else:
            dst_file_path = dst_path

        _limit = max(1, retry_limit)
        sftp = new_sftp if new_sftp else self.sftp
        retry_count = 1
        while retry_count < _limit:
            retry_count += 1
            try:
                sftp.put(src_file_path, dst_file_path)
                return dst_file_path
            except Exception as e:
                print(f"Put local {src_file_path} to {dst_file_path} on {self.ip} failed.")
                time.sleep(2)
        return None

    def close(self, channel=None):
        if channel is not None:
            channel.close()
        else:
            self.ssh.close()
