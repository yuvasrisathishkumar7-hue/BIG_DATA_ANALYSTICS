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

<hr>
<b>

SPOTIFY TAMIL MOVIE SONG ANALYTICS USING DISTRIBUTED MAPREDUCE ENGINE
WEEK 2 – BIG DATA ANALYTICS PROJECT
PROJECT OVERVIEW

This project implements a Distributed MapReduce Engine from Scratch using Python to process and analyze a Spotify-style dataset of Tamil movie songs.

The main objective of the project is to demonstrate how large-scale data can be processed efficiently using the MapReduce programming model. The system divides the input dataset into smaller chunks, processes these chunks using independent mapper processes, generates intermediate key-value pairs, distributes the records using a custom hash partitioner, stores the intermediate data on local disk, sorts the data, and finally performs parallel reduction to produce the final aggregated results.

For this project, the MapReduce engine is applied to Tamil movie song data to analyze the number of songs associated with each music director.

The project demonstrates the complete data-processing pipeline from input splitting to final aggregation.

OBJECTIVES

The main objectives of this project are:

To understand the fundamental working principles of MapReduce.
To design and implement a MapReduce engine from scratch.
To divide a large input dataset into smaller chunks.
To execute independent mapper processes in parallel.
To generate intermediate key-value pairs.
To implement custom hash-based partitioning.
To route the same keys to the same reducer partition.
To store intermediate key-value pairs on local disk.
To sort partitioned data before the reduction phase.
To execute independent reducer processes in parallel.
To group and aggregate values associated with each key.
To generate meaningful analytical results from the dataset.
PROBLEM STATEMENT

Processing a large dataset using a single sequential program can be time-consuming. The MapReduce programming model solves this problem by dividing the processing task into smaller independent operations that can be executed concurrently.

In this project, a Spotify-style Tamil movie song dataset is processed using a custom MapReduce engine. The input data is divided into multiple chunks, and each chunk is assigned to an independent mapper process.

The mapper extracts the music director from each song record and generates an intermediate key-value pair in the following format:

(Music Director, 1)

The generated intermediate records are then distributed among multiple reducers using a custom hash partitioning technique. The partitioned records are stored on local disk and sorted so that identical keys are grouped together.

Finally, reducer processes aggregate the values associated with each music director and calculate the total number of songs associated with each one.

DATASET DESCRIPTION

The input dataset represents Tamil movie song information in a Spotify-style format.

Each record contains information about:

Song Name
Music Director
Movie Name
INPUT FORMAT
Song Name,Music Director,Movie Name
SAMPLE INPUT
Why This Kolaveri Di,Anirudh Ravichander,3
Vaathi Coming,Anirudh Ravichander,Master
Arabic Kuthu,Anirudh Ravichander,Beast
Rowdy Baby,Yuvan Shankar Raja,Maari 2
Jimikki Ponnu,Thaman S,Varisu

The primary analytical task is:

Calculate the total number of songs associated with each music director.

SYSTEM ARCHITECTURE
                    SPOTIFY TAMIL SONG DATASET
                              │
                              ▼
                         input.txt
                              │
                              ▼
                       INPUT SPLITTING
                              │
               ┌──────────────┼──────────────┐
               ▼              ▼              ▼
            MAPPER 1       MAPPER 2       MAPPER N
               │              │              │
               └──────────────┼──────────────┘
                              ▼
                 INTERMEDIATE KEY-VALUE PAIRS
                      (Music Director, 1)
                              │
                              ▼
                    CUSTOM HASH PARTITIONING
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
              PARTITION 0         PARTITION 1
                    │                   │
                    └─────────┬─────────┘
                              ▼
                           SORTING
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
                 REDUCER 0           REDUCER 1
                    │                   │
                    └─────────┬─────────┘
                              ▼
                       FINAL AGGREGATION
                              │
                              ▼
                     FINAL OUTPUT FILE
MAPREDUCE WORKFLOW
1. INPUT SPLITTING

The input.txt file contains the Tamil movie song records.

The main program reads the input file and divides the records into smaller chunks.

For example:

Input Dataset
     │
     ├── Chunk 1 → Mapper 1
     ├── Chunk 2 → Mapper 2
     └── Chunk 3 → Mapper 3

Each chunk is independently processed by a mapper process.

This allows multiple parts of the dataset to be processed concurrently.

2. MAPPER PHASE

The mapper reads each song record and extracts the music director.

For every song, the mapper generates an intermediate key-value pair:

(Music Director, 1)
EXAMPLE

Input:

Vaathi Coming,Anirudh Ravichander,Master

Mapper output:

(Anirudh Ravichander, 1)

If the same music director appears in multiple records, the mapper generates multiple intermediate pairs.

The mapper only generates the intermediate data. It does not calculate the final total.

3. HASH PARTITIONING

After the mapping phase, the intermediate key-value pairs are distributed among reducer processes.

The project uses a custom hash partitioning method:

hash(key) % number_of_reducers

The result determines the reducer responsible for processing the key.

0 → Reducer 0
1 → Reducer 1

The main purpose of hash partitioning is to ensure that all occurrences of the same key are sent to the same reducer.

This is essential for correct aggregation.

4. INTERMEDIATE DATA STORAGE

The partitioned intermediate key-value records are stored on the local disk.

The project uses an intermediate directory:

intermediate/
├── partition_0.txt
└── partition_1.txt

Storing intermediate data demonstrates how a MapReduce system can maintain intermediate processing states before starting the reduction phase.

5. SORTING

Before the reducer starts processing the partition data, the records are sorted based on their keys.

Sorting ensures that identical keys are placed together, which makes grouping and reduction easier.

6. REDUCER PHASE

The reducer receives a key along with all the values associated with that key.

For example:

Anirudh Ravichander → [1, 1, 1, 1, 1]

The reducer calculates:

1 + 1 + 1 + 1 + 1 = 5

The final result becomes:

Anirudh Ravichander 5

The same process is performed for all music directors.

7. FINAL OUTPUT

After all reducer processes complete their execution, the final aggregated results are generated.

Example:

Anirudh Ravichander    5
Santhosh Narayanan     3
Thaman S               4
Yuvan Shankar Raja     6

The final results can be stored inside:

output/
└── final_output.txt
PROJECT STRUCTURE
Spotify_Tamil_Song_MapReduce/
│
├── main.py
├── mapper.py
├── partitioner.py
├── reducer.py
├── input.txt
├── README.md
│
├── intermediate/
│   ├── partition_0.txt
│   └── partition_1.txt
│
└── output/
    └── final_output.txt
FILE DESCRIPTION
FILE / DIRECTORY	DESCRIPTION
main.py	Controls and coordinates the complete MapReduce workflow
mapper.py	Converts input records into intermediate key-value pairs
partitioner.py	Implements custom hash-based partitioning
reducer.py	Aggregates values associated with each music director
input.txt	Contains Tamil movie song records
intermediate/	Stores intermediate partition files
output/	Stores final aggregated results
README.md	Contains project documentation
TECHNOLOGY STACK
Programming Language: Python
Parallel Processing: Python multiprocessing
Processing Model: MapReduce
Partitioning: Custom Hash Partitioning
Storage: Local File System
Development Environment: Visual Studio Code
Version Control: Git
Repository Hosting: GitHub
KEY FEATURES
PARALLEL MAPPER PROCESSING

Multiple mapper processes can process different input chunks independently.

CUSTOM HASH PARTITIONING

The system uses:

hash(key) % number_of_reducers

to distribute intermediate data among reducers.

INTERMEDIATE DISK STORAGE

The intermediate results are stored as partition files before the reduction stage.

SORTING

Partitioned records are sorted before reduction so that identical keys can be grouped efficiently.

PARALLEL REDUCER PROCESSING

Multiple reducers process different partitions independently.

DATA AGGREGATION

The reducer calculates the total number of songs associated with each music director.

MODULAR ARCHITECTURE

The project separates the mapper, partitioner, reducer, and main controller into individual Python files.

REQUIREMENT MAPPING
REQUIREMENT	IMPLEMENTATION
MapReduce Engine	main.py
Input Splitting	Input records divided into chunks
Parallel Mapper Processes	Python multiprocessing
Mapper Logic	mapper.py
Intermediate Key-Value Pairs	(Music Director, 1)
Custom Hash Partitioning	partitioner.py
Partition Formula	hash(key) % number_of_reducers
Intermediate Storage	intermediate/partition_*.txt
Sorting	Partition records sorted by key
Parallel Reducer Processes	Python multiprocessing
Aggregation	reducer.py
Final Results	output/final_output.txt
HOW TO RUN
PREREQUISITES

Install Python 3.x on your system.

Check the Python version:

python --version
STEP 1: NAVIGATE TO THE PROJECT DIRECTORY
cd WEEK-2/Spotify_Tamil_Song_MapReduce
STEP 2: RUN THE MAPREDUCE ENGINE
python main.py

The program performs:

Input Reading
      ↓
Input Splitting
      ↓
Parallel Mapping
      ↓
Intermediate Key-Value Generation
      ↓
Hash Partitioning
      ↓
Intermediate Disk Storage
      ↓
Sorting
      ↓
Parallel Reduction
      ↓
Final Aggregation
      ↓
Final Output
STEP 3: CHECK THE OUTPUT

The final output can be viewed in:

output/final_output.txt

The intermediate partition files can be viewed in:

intermediate/
SAMPLE OUTPUT
==================================================
SPOTIFY TAMIL MOVIE SONG MAPREDUCE ENGINE
==================================================

Input Split Completed
Mapper Processes Completed
Hash Partitioning Completed
Intermediate Data Stored
Sorting Completed
Reducer Processes Completed

==================================================
FINAL OUTPUT
==================================================

Anirudh Ravichander    5
Santhosh Narayanan     3
Thaman S               4
Yuvan Shankar Raja     6

==================================================
MAPREDUCE JOB COMPLETED SUCCESSFULLY
==================================================

Note: The exact output depends on the records available in input.txt.

LEARNING OUTCOMES

Through this project, the following concepts are practically explored:

MapReduce programming model.
Parallel data processing.
Python multiprocessing.
Input splitting.
Mapper and reducer architecture.
Intermediate key-value processing.
Custom hash partitioning.
Local disk-based intermediate storage.
Sorting and grouping.
Data aggregation.
Distributed processing concepts.
Modular Python application development.
FUTURE ENHANCEMENTS

The project can be extended with the following features:

Support for larger Tamil movie song datasets.
Dynamic configuration of mapper and reducer counts.
Artist-wise song analysis.
Movie-wise song analysis.
Year-wise song analysis.
Genre-based song analysis.
Song popularity analysis.
Rating-based analytics.
Interactive data visualization dashboards.
Integration with real Spotify datasets or APIs.
Distributed execution across multiple systems.
Integration with Apache Hadoop or Apache Spark.
CONCLUSION

The Spotify Tamil Movie Song Analytics Using Distributed MapReduce Engine project demonstrates the implementation of a simplified MapReduce framework from scratch using Python.

The system follows the complete MapReduce data-processing pipeline, beginning with input splitting and parallel mapping, followed by intermediate key-value generation, custom hash partitioning, local disk storage, sorting, and parallel reduction.

The project demonstrates how large datasets can be divided into smaller processing tasks and handled concurrently using independent mapper and reducer processes. By applying the MapReduce model to Tamil movie song data, the system efficiently aggregates song information based on music directors.

Overall, this project provides practical knowledge of parallel processing, distributed computing concepts, MapReduce architecture, hash partitioning, intermediate data management, sorting, and data aggregation, forming a strong foundation for understanding modern Big Data processing systems.

AUTHOR

S. YUVASRI
BACHELOR OF COMPUTER APPLICATIONS (BCA)
KAMARAJ COLLEGE, THOOTHUKUDI, TAMIL NADU
<b>
