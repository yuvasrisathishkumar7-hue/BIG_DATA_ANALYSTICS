"""
log_server_stimulator.py
-----------------------
Meesho Log Server Simulator

Simulates three Meesho backend servers:

1. Login Server
2. Order Server
3. Inventory Server

Each server continuously sends logs to the Log Harvester
through TCP sockets.

Run this file FIRST.
"""

import socket
import threading
import random
import time
from datetime import datetime

# ==========================================================
# Simulated Meesho Servers
# ==========================================================

SERVERS = [
    ("meesho-login-server", 9001),
    ("meesho-order-server", 9002),
    ("meesho-inventory-server", 9003),
]

LOG_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]

# ==========================================================
# Extra Realistic Data
# ==========================================================

LOCATIONS = [
    "Chennai",
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Madurai",
    "Coimbatore",
    "Pune"
]

PRODUCTS = [
    "Saree",
    "T-Shirt",
    "Shoes",
    "Kitchen Set",
    "Smart Watch",
    "Bedsheet",
    "Handbag",
    "Mobile Cover"
]

PAYMENTS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Cash on Delivery",
    "Wallet"
]

# ==========================================================
# Message Templates
# ==========================================================

MESSAGE_TEMPLATES = {

    "INFO": [
        "Customer {cid} from {city} logged in successfully",
        "Customer {cid} from {city} placed Order {oid} for {product} worth ₹{amount} using {payment}",
        "Order {oid} for {product} shipped to {city}",
        "Order {oid} delivered successfully in {city}",
        "Inventory updated for {product}",
    ],

    "WARNING": [
        "Low stock for {product}",
        "Order {oid} delivery delayed in {city}",
        "High order traffic detected in {city}",
        "Payment verification taking longer for Order {oid}",
    ],

    "ERROR": [
        "Payment failed via {payment} for Order {oid}",
        "Order {oid} cancelled due to unavailable {product}",
        "Inventory update failed for {product}",
        "Customer {cid} login failed from {city}",
        "Database connection lost while processing Order {oid}",
    ],

    "DEBUG": [
        "Cache refreshed for {product}",
        "Retrying inventory update for {product}",
        "Session validated for Customer {cid}",
        "Background synchronization completed",
    ]
}

# ==========================================================
# Generate One Log
# ==========================================================

def build_log_line(server_name):

    level = random.choice(LOG_LEVELS)

    customer_id = random.randint(1000, 9999)
    order_id = random.randint(10000, 99999)
    city = random.choice(LOCATIONS)
    product = random.choice(PRODUCTS)
    payment = random.choice(PAYMENTS)
    amount = random.randint(199, 4999)

    message = random.choice(MESSAGE_TEMPLATES[level]).format(
        cid=customer_id,
        oid=order_id,
        city=city,
        product=product,
        payment=payment,
        amount=amount
    )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"{timestamp} | {level} | {server_name} | {message}\n"

# ==========================================================
# Send Logs
# ==========================================================

def handle_client(conn, server_name):

    print(f"[{server_name}] Log Harvester Connected")

    try:

        while True:

            log = build_log_line(server_name)

            conn.sendall(log.encode("utf-8"))

            # Random delay
            time.sleep(random.uniform(0.10, 0.50))

            # Occasionally send invalid log
            if random.random() < 0.05:
                conn.sendall(b"INVALID_LOG_DATA\n")

    except (BrokenPipeError, ConnectionResetError):

        print(f"[{server_name}] Harvester Disconnected")

    finally:

        conn.close()

# ==========================================================
# Run Individual Server
# ==========================================================

def run_server(server_name, port):

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind(("127.0.0.1", port))

    server.listen(1)

    print(f"[{server_name}] Listening on Port {port}")

    while True:

        conn, addr = server.accept()

        threading.Thread(
            target=handle_client,
            args=(conn, server_name),
            daemon=True
        ).start()

# ==========================================================
# Main
# ==========================================================

if __name__ == "__main__":

    print("=" * 55)
    print("        MEESHO LOG SERVER SIMULATOR")
    print("=" * 55)

    for server_name, port in SERVERS:

        threading.Thread(
            target=run_server,
            args=(server_name, port),
            daemon=True
        ).start()

    print("\nAll Meesho servers are running...")
    print("Waiting for Log Harvester...\n")

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nSimulator stopped.")