#! /bin/bash
source /etc/profile
cur_path=$(cd `dirname $0`; pwd)
cd $cur_path
nohup java -DdevelopmentMode=true -server -Dapppath=$cur_path/ -Xms1024M -Xmx1024M -Xmn340M -Xrs -XX:+UseGCOverheadLimit -XX:+UseParallelGC -XX:ParallelGCThreads=8 -XX:+UseParallelOldGC -XX:MaxGCPauseMillis=100 -XX:+UseAdaptiveSizePolicy -Djava.ext.dirs=../lib/ com.qs.screen.sm_server.SMServer >> nohup.log 2>&1 &
echo $! > $cur_path/server.pid
echo "${cur_path} started"
