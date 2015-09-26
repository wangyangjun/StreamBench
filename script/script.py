#!/bin/bash

cd ~
sudo apt-get install -y git
git clone https://github.com/wangyangjun/RealtimeStreamBenchmark.git
cd RealtimeStreamBenchmark
python install-jdk.py
python install-zookeeper.py
