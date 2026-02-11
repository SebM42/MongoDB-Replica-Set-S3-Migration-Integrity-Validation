#!/bin/bash
chmod 400 /etc/mongo-keyfile
chmod 700 /etc

MONGO_DATA=/data/db

exec mongod --dbpath $MONGO_DATA --replSet rs0 --keyFile /etc/mongo-keyfile --bind_ip_all