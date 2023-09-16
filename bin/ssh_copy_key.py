#!/usr/bin/python
# _*_ coding: utf-8 _*_
# @Time    : 2023/9/17 1:22
# @Author  : Seven
# @File    : ssh_copy_key.py

from lib.common.constant import Node
from lib.common.ssh_connection import LongSSHConnection

# Step 1: Read host IPs from file
with open('/home/Seven/bin/host_ips', 'r') as file:
    hosts = file.read().splitlines()

# Step 2: Collect id_rsa.pub content from each host
rsa_pub_list = []
for host in hosts:
    ssh = LongSSHConnection(node=Node('Seven', password='123456', ip=host, port='22'))
    cmd = 'ls ~/.ssh/'
    out = ssh.execute_cmd(cmd)
    if 'id_rsa.pub' not in out:
        keygen_cmd = 'ssh-keygen -q -N "" -f /home/Seven/.ssh/id_rsa'
        ssh.execute_cmd(keygen_cmd)
    cat_rsa_pub_cmd = 'cat ~/.ssh/id_rsa.pub'
    cat_out = ssh.execute_cmd(cat_rsa_pub_cmd)
    first_line = cat_out.split('\n')[1]
    ssh_key = first_line.strip()
    rsa_pub_list.append(ssh_key)
print("pub_list:", rsa_pub_list)

# Step 3: Write id_rsa.pub content to authorized_keys file on each host
for host in hosts:
    ssh = LongSSHConnection(node=Node('Seven', password='123456', ip=host, port='22'))
    authorized_keys_path = '~/.ssh/authorized_keys'
    rsa_pub_content = '\n'.join(rsa_pub_list)
    append_cmd = f"echo '{rsa_pub_content}' > {authorized_keys_path}"
    ssh.execute_cmd(append_cmd)
    host_scan = 'ssh-keyscan -f /home/Seven/bin/host_ips > ~/.ssh/known_hosts'
    ssh.execute_cmd(host_scan)

