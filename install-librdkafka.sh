#! /bin/sh

sudo curl -Lk -o /opt/librdkafka-1.3.0.tar.gz https://github.com/edenhill/librdkafka/archive/v1.3.0.tar.gz &&
    sudo tar -xzf /opt/librdkafka-1.3.0.tar.gz -C /opt &&
    cd /opt/librdkafka-1.3.0 &&
    sudo ./configure --prefix /usr &&
    sudo make &&
    sudo make install &&
    sudo make clean &&
    sudo ./configure --clean
