#!/usr/bin/python
# _*_ coding: utf-8 _*_
# @Time    : 2023/9/16 21:17
# @Author  : Seven
# @File    : install_sshpass.py

import paramiko
from constant import Node
from ssh_connection import LongSSHConnection

# 读取文件内容
with open('/home/Seven/bin/host_ips', 'r') as file:
    hosts = file.read().splitlines()
    print(hosts)

# 遍历主机列表
for host in hosts:
    host = host.strip()
    print("host:", host)
    try:
        # SSH连接
        ssh = LongSSHConnection(Node('Seven', '123456', host, '22'))

        # 检查sshpass命令是否存在
        out = ssh.execute_cmd('command -v sshpass')

        if 'sshpass' in out:
            # sshpass存在，结束程序
            print(f'sshpass 在{host} 上存在，安装下一台。')
            ssh.close()
            continue
        else:
            # 下载、编译和安装sshpass
            commands = [
                'wget --no-check-certificate "https://sourceforge.net/projects/sshpass/files/sshpass/1.10/sshpass-1.10.tar.gz"',
                'tar xvfz sshpass-1.10.tar.gz',
                'cd sshpass-1.10',
                './configure',
                'make',
                'sudo make install'
            ]
            command = ' && '.join(commands)
            ssh.execute_cmd(command, timeout=300)

            # 再次检查sshpass是否安装成功
            out = ssh.execute_cmd('command -v sshpass')

            if 'sshpass' in out:
                print(f"主机 {host} 上的sshpass安装成功")
            else:
                print(f"主机 {host} 上的sshpass安装不成功")

        ssh.close()
    except paramiko.AuthenticationException:
        print(f"主机 {host} 的用户名或密码不正确")
    except paramiko.SSHException as e:
        print(f"主机 {host} SSH连接失败: {str(e)}")
