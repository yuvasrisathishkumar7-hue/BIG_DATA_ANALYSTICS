"""
log_harvester_daemon.py
-------------------------------------------------------------
MEESHO CENTRAL LOG HARVESTER

Features
--------
1. Connects to multiple Meesho simulated servers.
2. Reads TCP byte streams continuously.
3. Performs manual socket stream buffering.
4. Validates each log using Regular Expressions.
5. Creates structured payloads.
6. Stores logs into dynamic binary partitions.
7. Displays live monitoring statistics.

Run log_server_simulator.py first.
Then execute this file.
"""

import os
import re
import socket
import struct
import threading
import time

from collections import defaultdict

# ==========================================================
# MEESHO SERVER CONFIGURATION
# ==========================================================

HOST = "127.0.0.1"

MEESHO_SERVERS = [

    ("meesho-login-server", 9001),

    ("meesho-order-server", 9002),

    ("meesho-inventory-server", 9003)

]

# Folder where binary partitions are stored

PARTITION_DIRECTORY = "meesho_partitions"

# ==========================================================
# LOG FORMAT
#
# Example
#
# 2026-07-10 14:25:32 | INFO |
# meesho-order-server |
# Customer 2345 from Chennai placed Order 51234
# for Saree worth ₹899 using UPI
# ==========================================================

LOG_PATTERN = re.compile(

    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*\|\s*"

    r"(?P<level>INFO|WARNING|ERROR|DEBUG)\s*\|\s*"

    r"(?P<service>[\w\-]+)\s*\|\s*"

    r"(?P<message>.+)$"

)

# ==========================================================
# BINARY LOG LEVEL CODES
# ==========================================================

LEVEL_CODES = {

    "DEBUG": 0,

    "INFO": 1,

    "WARNING": 2,

    "ERROR": 3

}

CODE_LEVELS = {

    value: key

    for key, value in LEVEL_CODES.items()

}

# ==========================================================
# PARTITION MANAGEMENT
# ==========================================================

partition_files = {}

partition_file_locks = defaultdict(threading.Lock)

partition_master_lock = threading.Lock()

# ==========================================================
# LIVE STATISTICS
# ==========================================================

statistics = defaultdict(int)

statistics_lock = threading.Lock()

# ==========================================================
# CREATE / OPEN PARTITION FILE
# ==========================================================

def get_partition(service_name, log_level):

    key = (service_name, log_level)

    with partition_master_lock:

        if key not in partition_files:

            os.makedirs(
                PARTITION_DIRECTORY,
                exist_ok=True
            )

            filename = os.path.join(

                PARTITION_DIRECTORY,

                f"{service_name}_{log_level}.bin"

            )

            partition_files[key] = open(

                filename,

                "ab"

            )

            print(
                f"[NEW PARTITION] {filename}"
            )

        return partition_files[key]


# ==========================================================
# BUILD CUSTOM BINARY RECORD
# ==========================================================

def build_binary_record(

    timestamp,

    level,

    service,

    message

):

    timestamp_bytes = (

        timestamp.encode("ascii")

        .ljust(19, b" ")

    )[:19]

    level_byte = LEVEL_CODES[level]

    service_bytes = service.encode("utf-8")

    message_bytes = message.encode("utf-8")

    header = struct.pack(

        "!19sBH",

        timestamp_bytes,

        level_byte,

        len(service_bytes)

    )

    message_header = struct.pack(

        "!H",

        len(message_bytes)

    )

    binary_record = (

        header +

        service_bytes +

        message_header +

        message_bytes

    )

    return binary_record
# ==========================================================
# STORE STRUCTURED LOG RECORD
# ==========================================================

def store_log_record(payload):
    """
    Converts a structured payload into a binary record and
    writes it into the correct partition file.
    """

    binary_record = build_binary_record(
        payload["timestamp"],
        payload["level"],
        payload["service"],
        payload["message"]
    )

    # Prefix every record with its length (4 bytes)
    length_prefix = struct.pack(
        "!I",
        len(binary_record)
    )

    key = (
        payload["service"],
        payload["level"]
    )

    partition = get_partition(
        payload["service"],
        payload["level"]
    )

    with partition_file_locks[key]:

        partition.write(
            length_prefix + binary_record
        )

        partition.flush()


# ==========================================================
# VALIDATE AND PROCESS ONE LOG
# ==========================================================

def process_log(raw_log, server_name):
    """
    Validates one incoming log line.

    If valid:
        -> Build payload
        -> Store in binary partition
        -> Update statistics

    If invalid:
        -> Reject
    """

    match = LOG_PATTERN.match(raw_log)

    if not match:

        with statistics_lock:
            statistics[(server_name, "REJECTED")] += 1

        return

    payload = {

        "timestamp": match.group("timestamp"),

        "level": match.group("level"),

        "service": match.group("service"),

        "message": match.group("message")

    }

    store_log_record(payload)

    with statistics_lock:

        statistics[
            (
                server_name,
                payload["level"]
            )
        ] += 1

    # Optional console output
    print(
        f"[{payload['level']}] "
        f"{payload['service']} -> "
        f"{payload['message']}"
    )


# ==========================================================
# LIVE MONITORING DASHBOARD
# ==========================================================

def show_live_statistics():

    while True:

        time.sleep(3)

        with statistics_lock:

            if not statistics:
                continue

            print("\n" + "=" * 70)
            print("             MEESHO LIVE LOG DASHBOARD")
            print("=" * 70)

            for (server, level), count in sorted(statistics.items()):

                print(
                    f"{server:30s}"
                    f"{level:10s}"
                    f"{count}"
                )

            print("=" * 70)
            print()
            # ==========================================================
# COLLECT LOGS FROM ONE MEESHO SERVER
# ==========================================================

def collect_server_logs(server_name, port):
    """
    Connects to one Meesho server and continuously
    receives logs.

    TCP delivers a stream of bytes rather than complete
    messages, so we maintain a rolling buffer and
    manually split complete log lines.
    """

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:

        client.connect((HOST, port))

        print(f"[CONNECTED] {server_name} ({port})")

        # Rolling TCP buffer
        buffer = b""

        while True:

            chunk = client.recv(4096)

            if not chunk:
                print(f"[{server_name}] Connection Closed")
                break

            buffer += chunk

            # Extract every complete log line
            while b"\n" in buffer:

                line_bytes, buffer = buffer.split(b"\n", 1)

                try:
                    log_line = line_bytes.decode("utf-8").strip()

                except UnicodeDecodeError:
                    continue

                if log_line:
                    process_log(log_line, server_name)

    except ConnectionRefusedError:

        print(f"[ERROR] Unable to connect to {server_name}")

    except Exception as error:

        print(f"[ERROR] {server_name}: {error}")

    finally:

        client.close()


# ==========================================================
# MAIN PROGRAM
# ==========================================================

if __name__ == "__main__":

    print("=" * 70)
    print("          MEESHO CENTRAL LOG HARVESTER")
    print("=" * 70)

    print("\nConnecting to simulated Meesho servers...\n")

    worker_threads = []

    # One harvesting thread per server
    for server_name, port in MEESHO_SERVERS:

        thread = threading.Thread(
            target=collect_server_logs,
            args=(server_name, port),
            daemon=True
        )

        thread.start()

        worker_threads.append(thread)

    # Dashboard thread
    dashboard_thread = threading.Thread(
        target=show_live_statistics,
        daemon=True
    )

    dashboard_thread.start()

    print("Log Harvester Running...")
    print("Press Ctrl + C to Stop.\n")

    try:

        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        print("\nStopping Log Harvester...")

        # Close every partition file
        for file_handle in partition_files.values():

            try:
                file_handle.close()
            except Exception:
                pass

        print("All partition files closed.")
        print("Shutdown completed successfully.")