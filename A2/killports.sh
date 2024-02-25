#!/bin/bash

# Check if ports list is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <port_list>"
    exit 1
fi

# Split the ports string into an array
IFS=' ' read -r -a ports <<< "$1"

# Loop through each port
for port in "${ports[@]}"; do
    # Check if the port is a number
    if ! [[ "$port" =~ ^[0-9]+$ ]]; then
        echo "Port '$port' is not a valid port number."
        continue
    fi

    # Find the process listening on the port
    pid=$(lsof -t -i :$port)
    
    # Check if a process is listening on the port
    if [ -z "$pid" ]; then
        echo "No process found listening on port $port"
    else
        # Kill the process
        echo "Killing process $pid listening on port $port"
        kill -9 "$pid"
    fi
done
