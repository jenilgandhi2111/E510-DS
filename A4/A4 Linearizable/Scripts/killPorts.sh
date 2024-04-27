#!/bin/bash

# Define the ports to kill
ports=(5001 5000 5002 7000 7001 7002 7003 5003 6001 6000 6002 6003)

# Loop through each port and kill the associated processes
for port in "${ports[@]}"; do
    echo "Killing processes on port $port"
    lsof -ti :$port | xargs -r kill -9
done
