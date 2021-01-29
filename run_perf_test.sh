#!/bin/bash

# Build the project
ant

cleanup_and_start_server() {
  rm KeyValueData.txt
  server_start="java -jar m1-server.jar 5000 1000 LRU"

  $server_start &>/dev/null &
  SERVER_PID=$!

  sleep 5
}

kill_server() {
  kill -9 $SERVER_PID &>/dev/null
}

# Run 3 types of tests
printf "Starting test 1 \n"
cleanup_and_start_server
java -jar perf-testing.jar 0.2 5 10000
kill_server

printf "Starting test 2 \n"
cleanup_and_start_server
java -jar perf-testing.jar 0.5 5 10000
kill_server

printf "Starting test 3 \n"
cleanup_and_start_server
java -jar perf-testing.jar 0.8 5 10000
kill_server