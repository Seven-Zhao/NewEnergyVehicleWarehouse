#!/bin/bash
set -x
[ "$(whoami)" = "root" ] || { echo "请以root权限执行此脚本"; exit 1; }
cd "$(dirname "$0")"
test -f mysql-community-client-8.0.31-1.el7.x86_64.rpm && \
test -f mysql-community-client-plugins-8.0.31-1.el7.x86_64.rpm && \
test -f mysql-community-common-8.0.31-1.el7.x86_64.rpm && \
test -f mysql-community-icu-data-files-8.0.31-1.el7.x86_64.rpm && \
test -f mysql-community-libs-8.0.31-1.el7.x86_64.rpm && \
test -f mysql-community-libs-compat-8.0.31-1.el7.x86_64.rpm && \
test -f mysql-community-server-8.0.31-1.el7.x86_64.rpm || { echo "请将所有MySQL-8.0.31的RPM安装包放到脚本相同目录"; exit 1; }

# 卸载MySQL
systemctl stop mysql mysqld 2>/dev/null
rpm -qa | grep -i 'mysql\|mariadb' | xargs -n1 rpm -e --nodeps 2>/dev/null
rm -rf /var/lib/mysql /var/log/mysqld.log /usr/lib64/mysql /etc/my.cnf /usr/my.cnf

set -e
# 安装并启动MySQL
yum install -y \
mysql-community-client-8.0.31-1.el7.x86_64.rpm \
mysql-community-client-plugins-8.0.31-1.el7.x86_64.rpm \
mysql-community-common-8.0.31-1.el7.x86_64.rpm \
mysql-community-icu-data-files-8.0.31-1.el7.x86_64.rpm \
mysql-community-libs-8.0.31-1.el7.x86_64.rpm \
mysql-community-libs-compat-8.0.31-1.el7.x86_64.rpm \
mysql-community-server-8.0.31-1.el7.x86_64.rpm >/dev/null 2>&1
systemctl start mysqld

#更改密码级别并重启MySQL
sed -i '/\[mysqld\]/avalidate_password.length=4\nvalidate_password.policy=0' /etc/my.cnf
systemctl restart mysqld

# 更改MySQL配置
tpass=$(cat /var/log/mysqld.log | grep "temporary password" | awk '{print $NF}')
cat << EOF | mysql -uroot -p"${tpass}" --connect-expired-password >/dev/null 2>&1
set password='000000';
update mysql.user set host='%' where user='root';
alter user 'root'@'%' identified with mysql_native_password by '000000';
flush privileges;
EOF
