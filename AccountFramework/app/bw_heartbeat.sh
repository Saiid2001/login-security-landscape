# heartbeat for bitwarden server
# try requesting /status
# if it fails, restart the server

BW_PASSWORD=$1

# function for starting the server
start_server() {
    bw login --apikey
    bw serve --port 9999 --hostname 0.0.0.0 &

    # Wait for BW API to start
    sleep 10

    # Unlock BW API
    curl -X POST http://0.0.0.0:9999/unlock -d "{\"password\": \"${BW_PASSWORD}\"}" -H 'Content-Type: application/json'
}

# function for checking the server status
check_status() {
    
    # make sure the response contains "data"
    if curl -s http://0.0.0.0:9999/list/object/items | grep -q "\"success\":true"; then
        echo "BW Server is running"
    else
        echo "BW Server is down, restarting"
        pid=$(ps aux | grep "bw serve" | grep -v grep | awk '{print $2}')
        kill -9 $pid
        start_server 

    fi
}

# start the server
start_server 

# check the server status every 5 minutes
while true; do
    check_status
    sleep 300
done