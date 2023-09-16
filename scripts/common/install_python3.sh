#!/bin/bash

# Step 1: 安装编译依赖
sudo yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel readline-devel sqlite-devel

# 添加 Python 路径到环境变量配置文件
add_python_path() {
    PYTHON_PATH="/opt/module/python/bin"
    PROFILE_FILE="/etc/profile.d/my_env.sh"

    # 检查是否已存在路径配置
    if grep -q "$PYTHON_PATH" "$PROFILE_FILE"; then
        echo "Python path already exists in $PROFILE_FILE"
    else
        # 添加路径配置
        echo "export PATH=\$PATH:$PYTHON_PATH" | sudo tee -a "$PROFILE_FILE" > /dev/null
        echo "Python path added to $PROFILE_FILE"
    fi
}

# Step 2: 判断当前Python环境版本
python_version=$(python -V 2>&1 | awk '{print $2}' | cut -d'.' -f1)
if [ "$python_version" != "3" ]; then
  # Step 3: 判断Python源码文件是否存在
  if [ ! -e "Python-3.11.5.tar.xz" ]; then
    # 下载Python源码文件
    wget https://www.python.org/ftp/python/3.11.5/Python-3.11.5.tar.xz
  fi

  # Step 4: 解压Python源码文件
  tar -xf Python-3.11.5.tar.xz
  # shellcheck disable=SC2164
  cd Python-3.11.5

  # Step 5: 配置编译选项
  mkdir -p /opt/module/python
  ./configure --prefix=/opt/module/python --with-ssl

  # Step 6: 编译并安装Python
  sudo make && sudo make install

  # Step 7: 检查python3命令是否可用
  # 删除旧的python符号链接
  sudo rm -f /usr/bin/python
  # 建立新的符号链接
  sudo ln -s /opt/module/python/bin/python3 /usr/bin/python
  # 配置python环境变量
  add_python_path
  sudo ln -s /opt/module/python/bin/pip3 /usr/bin/pip
else
  echo "Current Python version is already Python 3"
fi
