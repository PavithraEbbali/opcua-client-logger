# OPC UA Client Logger

This project implements an **OPC UA Client** that connects to a simulated OPC UA server, reads dummy tags at regular intervals, and logs the data into **hourly CSV spreadsheets**.  

---

## 🚀 Features
- Connects to **Prosys OPC UA Simulation Server** (or other OPC UA servers).
- Reads **10 tags** (7 unique simulation tags + 3 repeats to make 10).
- Logs data every **minute** (configurable).
- Creates a **new CSV file each hour** automatically.
- Each row includes:
  - `Timestamp` (HH:MM:SS)
  - `Epoch Time` (UTC)
  - 10 tag values
- Handles **server disconnects** with auto-reconnect.

---

## 📂 Project Structure

```yaml
project_structure:
  root: opcua-client-logger/
  files:
    - app.py: "Main OPC UA client script"
    - requirements.txt: "Python dependencies"
    - README.md: "Instructions file"
  folders:
    logs:
      description: "Log files are stored here"
      files:
        - OPC_Log_2025-08-31_07.csv
        - OPC_Log_2025-08-31_08.csv
```
---

## setup_instructions:
  - step: "Install Python"
    <br> details: |
      Make sure Python 3.8+ is installed:
      ```bash
      python --version
      ```

  - step: "Install Dependencies"
    <br> details: |
      Inside the project folder, run:
      ```bash
      pip install -r requirements.txt
      ```

  - step: "Install OPC UA Simulator"
    <br> details: |
      Download and install **Prosys OPC UA Simulation Server** (free version).
      Run it → the default endpoint is usually:
      ```
      opc.tcp://localhost:53530/OPCUA/SimulationServer
      ```

      Simulation tags available:
        - Counter (`ns=3;i=1001`)
        - Random (`ns=3;i=1002`)
        - Sawtooth (`ns=3;i=1003`)
        - Sinusoid (`ns=3;i=1004`)
        - Square (`ns=3;i=1005`)
        - Triangle (`ns=3;i=1006`)
        - Constant (`ns=3;i=1007`)

  - step: "Run the Client"
    <br> details: |
      ```bash
      python app.py
      ```

      The client will connect to the server.
      Data will be logged into `/logs/OPC_Log_YYYY-MM-DD_HH.csv`.
      A new file is created automatically at every hour.

sample_output:
  <br> description: "Example row in the generated CSV file"
  <br> csv_row: |
    Timestamp,Epoch,Tag1,Tag2,Tag3,Tag4,Tag5,Tag6,Tag7,Tag8,Tag9,Tag10
    07:28:12,1756605492,9,1.261108,0.8,1.175571,2.0,0.8000001,1.0,9,1.261108,0.8
    07:29:12,1756605552,7,-1.765223,0.8,1.175571,-2.0,0.8000001,1.0,7,-1.765223,0.8
