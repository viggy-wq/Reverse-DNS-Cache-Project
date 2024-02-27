#!/bin/bash

# Get the current directory
current_dir=$(pwd)

# Command to launch Mosquitto
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; /usr/local/opt/mosquitto/sbin/mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf"
end tell
END

# Delay to ensure commands don't overlap (optional, adjust as necessary)
sleep 1

# Command to run subscriber_handler.py
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; python3 subscriber_handler.py"
end tell
END

# Delay to ensure commands don't overlap
sleep 1

# Command to run request_server_interface.py
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; python3 request_server_interface.py"
end tell
END

# Delay to ensure commands don't overlap
sleep 1

# Command to run test.py
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; python3 test.py"
end tell
END


# Command to run rsi_test.py
osascript <<END
tell application "Terminal"
    do script "cd '$current_dir'; python3 rsi_test.py"
end tell
END


