#!/bin/bash

consumer_pids=$(ps ax | grep -e "python3 -u consumer" | grep -v "grep" | grep -E "^\s*?[0-9]+" -o | tr "\n" " ")

if [ -z "${consumer_pids}" ]; then
  echo "consumer is not running!"
else
  echo "killing consumer with pid $consumer_pids"
  kill $consumer_pids
fi

nohup python3 -u consumer.py >> out/partition_x.txt &
echo ""
nohup python3 -u consumer.py >> out/partition_y.txt &
echo ""
nohup python3 -u consumer.py >> out/partition_z.txt &