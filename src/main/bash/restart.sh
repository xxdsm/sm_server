#!/bin/sh
cur_path=$(cd `dirname $0`; pwd)
cd $cur_path
./stop.sh
./start.sh
