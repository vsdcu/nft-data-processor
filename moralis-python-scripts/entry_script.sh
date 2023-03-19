#!/bin/bash

echo '' > current_processing
# Set the default number of processes to 4 if no argument is provided
if [ -z "$1" ]
then
    num_processes=4
else
    num_processes=$1
fi

# Start the processes
for ((i=1; i<=$num_processes; i++))
do
    # Start a new instance of the MoralisNftContract.py script in the background and redirect output to a log file with the PID in the name
    pid=$$
    log_file="log-$i-$pid.txt"
    echo "Process $i started. PID: $pid" >> "$log_file"
    python3 MoralisApiNftContract.py >> "$log_file" 2>&1 &
done

python3 MoralisApiNftTransfers.py >> NFT_TRANSFERS.log 2>&1 &
python3 MoralisApiOpenSeaTrades.py >> OPEN_SEA_TRADES.log 2>&1 &

# Loop to check if any process has stopped and restart it
while true
do
    for ((i=1; i<=$num_processes; i++))
    do
        pid=$(pgrep -f "MoralisNftContract.py" | sed -n "$i"p)
        if [ -z "$pid" ]
        then
            # Start a new instance of the MoralisNftContract.py script in the background and redirect output to a log file with the PID in the name
            pid=$$
            log_file="log-$i-$pid.txt"
            echo "Process $i restarted. PID: $pid" >> "$log_file"
            python3 MoralisNftContract.py >> "$log_file" 2>&1 &
        fi
    done
    sleep 10
done
