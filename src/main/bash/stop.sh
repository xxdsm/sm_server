#!/bin/sh
cur_path=$(cd `dirname $0`; pwd)
cd $cur_path
if [ ! -d $cur_path/outback ]
then
	mkdir -p $cur_path/outback
fi
kill -9 $(cat $cur_path/server.pid)
mv $cur_path/nohup.log $cur_path/outback/nohup.`date +%Y-%m-%d-%H-%M`.log
echo "${cur_path} stoped"
