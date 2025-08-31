import time
import os
import pandas as pd
from datetime import datetime
from opcua import Client

# ==========================================================
# CONFIGURATION
# ==========================================================
SERVER_URL = "opc.tcp://Pavithra:53530/OPCUA/SimulationServer"
LOG_INTERVAL = 60  # seconds (default: 1 minute)

# 7 available tags in your Simulation folder + 3 repeats (total 10)
TAGS = [
    "ns=3;i=1001",  # Counter
    "ns=3;i=1002",  # Random
    "ns=3;i=1003",  # Sawtooth
    "ns=3;i=1004",  # Sinusoid
    "ns=3;i=1005",  # Square
    "ns=3;i=1006",  # Triangle
    "ns=3;i=1007",  # Constant
    "ns=3;i=1001",  # Counter again
    "ns=3;i=1002",  # Random again
    "ns=3;i=1003"   # Sawtooth again
]

# Create logs folder if not exists
if not os.path.exists("logs"):
    os.makedirs("logs")

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================
def get_current_log_filename():
    """Return filename based on current date and hour."""
    now = datetime.now()
    return f"logs/OPC_Log_{now.strftime('%Y-%m-%d_%H')}.csv"

def connect_client():
    """Connect to OPC UA Server with retry logic."""
    while True:
        try:
            client = Client(SERVER_URL)
            client.connect()
            print(f"✅ Connected to {SERVER_URL}")
            return client
        except Exception as e:
            print(f"⚠️ Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# ==========================================================
# MAIN LOOP
# ==========================================================
def main():
    client = connect_client()

    try:
        while True:
            now = datetime.now()
            timestamp_str = now.strftime("%H:%M:%S")
            timestamp_epoch = int(time.time())

            # Read tag values
            values = []
            for tag in TAGS:
                node = client.get_node(tag)
                try:
                    val = node.get_value()
                except Exception:
                    val = None
                values.append(val)

            # Prepare row + headers
            row = [timestamp_str, timestamp_epoch] + values
            header = ["Timestamp", "Epoch"] + [f"Tag{i+1}" for i in range(len(TAGS))]

            # Create dataframe with headers
            df = pd.DataFrame([row], columns=header)

            # File handling
            filename = get_current_log_filename()
            file_exists = os.path.isfile(filename)

            # Save row to CSV
            df.to_csv(filename, mode='a', header=not file_exists, index=False)

            print(f"📌 Logged at {timestamp_str} → {values}")

            # Wait before next log
            time.sleep(LOG_INTERVAL)

    except KeyboardInterrupt:
        print("🛑 Stopped by user.")
    except Exception as e:
        print(f"❌ Error occurred: {e}. Restarting client...")
        client.disconnect()
        main()  # restart on failure
    finally:
        client.disconnect()
        print("🔌 Disconnected.")

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    main()
