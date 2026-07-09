# REAL-TIME PARALLEL LOG AGGREGATION ENGINE WITH CUSTOM SOCKET SLICING

## What this project actually does (plain English)

Imagine 3 Meesho backend servers (**Login Server, Order Server, Inventory Server**) continuously generating logs such as customer login, order placement, payment status, inventory updates, and delivery information. Our job is to build a **Log Harvester Daemon** (background program) that:

1. Connects to all 3 Meesho servers **simultaneously** using multi-threading.
2. Collects logs continuously through **TCP socket communication**.
3. Splits the incoming TCP byte stream into complete log messages (this is the **custom socket slicing** part because TCP sends a continuous stream of bytes, not individual messages).
4. Validates each log using **Regular Expressions (Regex)**.
5. Organizes valid logs into separate binary files based on **Server + Log Level** (dynamic partitioning).
6. Stores the processed logs in a compact **binary format** for efficient storage and retrieval.

## Files

* `log_server_simulator.py` — Simulates three Meesho backend servers generating logs continuously.
* `log_harvester_daemon.py` — **The main assignment deliverable** that connects to servers, performs socket slicing, validates logs, partitions them, and stores them in binary format.
* `read_binary_logs.py` — Reads binary partition files and converts them back into readable log records.
* `meesho_partitions/` — Output folder automatically created by the harvester, containing one `.bin` file for each **Server + Log Level** combination.

## How to run it (2 terminals)

**Terminal 1**

```bash
python log_server_simulator.py
```

Leave this terminal running.

**Terminal 2**

```bash
python log_harvester_daemon.py
```

The harvester connects to all Meesho servers, collects logs, validates them, partitions them, and stores them into binary files.

**To verify the output**

```bash
python read_binary_logs.py meesho_partitions/meesho-order-server_ERROR.bin
```

Replace the filename with any `.bin` file inside the `meesho_partitions` folder.

## How each requirement in the task description maps to the code

| Requirement                                     | Where it lives                                                         |
| ----------------------------------------------- | ---------------------------------------------------------------------- |
| Multi-threaded log harvesting daemon            | `log_harvester_daemon.py` – One `threading.Thread` per Meesho server   |
| Opens TCP sockets to monitor server instances   | `collect_server_logs()` using `socket.connect((HOST, port))`           |
| Parse stream buffers in real time               | `buffer += chunk` and `while b"\n" in buffer` loop                     |
| Execute regular expression validations          | `LOG_PATTERN` and `process_log()`                                      |
| Partition logs into dynamic structured payloads | `payload` dictionary and `get_partition()` function                    |
| Write to partitioned local raw binary buffers   | `build_binary_record()` using `struct.pack()` and `store_log_record()` |

## Why this design (in case you're asked "why not just use JSON/CSV?")

* TCP is a **stream protocol**, so incoming data must be buffered and manually split into complete log messages. This demonstrates **custom socket slicing**.
* Binary encoding using `struct.pack()` is more compact and faster than storing logs as plain text, making it suitable for high-volume log processing.
* Partitioning logs by **Server + Log Level** allows faster retrieval of specific logs without searching through a single large file.

## Things worth mentioning if faculty asks follow-up questions

* **Thread Safety:** Each partition file uses its own `threading.Lock()` to prevent concurrent write conflicts.
* **Malformed Log Handling:** The simulator occasionally sends invalid log entries. These fail Regex validation, are rejected, and are counted as **REJECTED** in the live statistics.
* **Scalability:** Additional Meesho servers can be added by updating the server list without changing the core harvesting logic, demonstrating dynamic scalability.
