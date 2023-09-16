#!/bin/bash

# Step 1: Check if ~/.ssh/id_rsa.pub file exists
if [ ! -f ~/.ssh/id_rsa.pub ]; then
  # File does not exist, generate SSH key pair
  ssh-keygen -t rsa <<<$'\n\n\n'
fi

# Step 2: Determine host list
if [[ "$1" == "-h="* ]]; then
  hosts=$(echo ${1#*=} | tr ',' '\n')
else
  hosts=$(cat /home/Seven/bin/host_ips)
fi

# Step 3: Copy public key to each host
for host in $hosts; do
  password=$(cat /home/Seven/bin/passwd)
  sshpass -p "$password" ssh-copy-id -o StrictHostKeyChecking=no $host
done
