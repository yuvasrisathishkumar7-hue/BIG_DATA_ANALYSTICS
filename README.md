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

<h1 align="center">SPOTIFY TAMIL MOVIE SONG ANALYTICS USING DISTRIBUTED MAPREDUCE ENGINE</h1>

<h2 align="center">WEEK 2 – BIG DATA ANALYTICS PROJECT</h2>

<hr>

<h2>PROJECT OVERVIEW</h2>

<p>
This project implements a <strong>Distributed MapReduce Engine from Scratch</strong> using Python to process and analyze a Spotify-style dataset of Tamil movie songs.
</p>

<p>
The main objective of the project is to demonstrate how large-scale data can be processed efficiently using the <strong>MapReduce programming model</strong>. The system divides the input dataset into smaller chunks, processes these chunks using independent mapper processes, generates intermediate key-value pairs, distributes the records using a custom hash partitioner, stores the intermediate data on local disk, sorts the data, and finally performs parallel reduction to produce the final aggregated results.
</p>

<p>
For this project, the MapReduce engine is applied to Tamil movie song data to analyze the <strong>number of songs associated with each music director</strong>.
</p>

<p>
The project demonstrates the complete data-processing pipeline from <strong>input splitting to final aggregation</strong>.
</p>

<hr>

<h2>OBJECTIVES</h2>

<ul>
<li>To understand the fundamental working principles of MapReduce.</li>
<li>To design and implement a MapReduce engine from scratch.</li>
<li>To divide a large input dataset into smaller chunks.</li>
<li>To execute independent mapper processes in parallel.</li>
<li>To generate intermediate key-value pairs.</li>
<li>To implement custom hash-based partitioning.</li>
<li>To route the same keys to the same reducer partition.</li>
<li>To store intermediate key-value pairs on local disk.</li>
<li>To sort partitioned data before the reduction phase.</li>
<li>To execute independent reducer processes in parallel.</li>
<li>To group and aggregate values associated with each key.</li>
<li>To generate meaningful analytical results from the dataset.</li>
</ul>

<hr>

<h2>PROBLEM STATEMENT</h2>

<p>
Processing a large dataset using a single sequential program can be time-consuming. The MapReduce programming model solves this problem by dividing the processing task into smaller independent operations that can be executed concurrently.
</p>

<p>
In this project, a Spotify-style Tamil movie song dataset is processed using a custom MapReduce engine. The input data is divided into multiple chunks, and each chunk is assigned to an independent mapper process.
</p>

<p>
The mapper extracts the music director from each song record and generates an intermediate key-value pair in the following format:
</p>

<pre>
(Music Director, 1)
</pre>

<p>
The generated intermediate records are then distributed among multiple reducers using a custom hash partitioning technique. The partitioned records are stored on local disk and sorted so that identical keys are grouped together.
</p>

<p>
Finally, reducer processes aggregate the values associated with each music director and calculate the total number of songs associated with each one.
</p>

<hr>

<h2>DATASET DESCRIPTION</h2>

<p>
The input dataset represents Tamil movie song information in a Spotify-style format.
</p>

<p><strong>Each record contains information about:</strong></p>

<ul>
<li>Song Name</li>
<li>Music Director</li>
<li>Movie Name</li>
</ul>

<h3>INPUT FORMAT</h3>

<pre>
Song Name,Music Director,Movie Name
</pre>

<h3>SAMPLE INPUT</h3>

<pre>
Why This Kolaveri Di,Anirudh Ravichander,3
Vaathi Coming,Anirudh Ravichander,Master
Arabic Kuthu,Anirudh Ravichander,Beast
Rowdy Baby,Yuvan Shankar Raja,Maari 2
Jimikki Ponnu,Thaman S,Varisu
</pre>

<p><strong>Primary Analytical Task:</strong></p>

<blockquote>
Calculate the total number of songs associated with each music director.
</blockquote>

<hr>

<h2>SYSTEM ARCHITECTURE</h2>

<pre>
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
</pre>

<hr>

<h2>MAPREDUCE WORKFLOW</h2>

<h3>1. INPUT SPLITTING</h3>

<p>
The <code>input.txt</code> file contains the Tamil movie song records.
</p>

<p>
The main program reads the input file and divides the records into smaller chunks.
</p>

<pre>
Input Dataset
     │
     ├── Chunk 1 → Mapper 1
     ├── Chunk 2 → Mapper 2
     └── Chunk 3 → Mapper 3
</pre>

<p>
Each chunk is independently processed by a mapper process. This allows multiple parts of the dataset to be processed concurrently.
</p>

<h3>2. MAPPER PHASE</h3>

<p>
The mapper reads each song record and extracts the music director.
</p>

<p>
For every song, the mapper generates an intermediate key-value pair:
</p>

<pre>
(Music Director, 1)
</pre>

<h4>EXAMPLE</h4>

<p><strong>Input:</strong></p>

<pre>
Vaathi Coming,Anirudh Ravichander,Master
</pre>

<p><strong>Mapper Output:</strong></p>

<pre>
(Anirudh Ravichander, 1)
</pre>

<p>
If the same music director appears in multiple records, the mapper generates multiple intermediate pairs.
</p>

<p>
The mapper only generates the intermediate data. It does not calculate the final total.
</p>

<h3>3. HASH PARTITIONING</h3>

<p>
After the mapping phase, the intermediate key-value pairs are distributed among reducer processes.
</p>

<p>
The project uses a custom hash partitioning method:
</p>

<pre>
hash(key) % number_of_reducers
</pre>

<p>
The result determines the reducer responsible for processing the key.
</p>

<pre>
0 → Reducer 0
1 → Reducer 1
</pre>

<p>
The main purpose of hash partitioning is to ensure that <strong>all occurrences of the same key are sent to the same reducer</strong>.
</p>

<p>
This is essential for correct aggregation.
</p>

<h3>4. INTERMEDIATE DATA STORAGE</h3>

<p>
The partitioned intermediate key-value records are stored on the local disk.
</p>

<pre>
intermediate/
├── partition_0.txt
└── partition_1.txt
</pre>

<p>
Storing intermediate data demonstrates how a MapReduce system can maintain intermediate processing states before starting the reduction phase.
</p>

<h3>5. SORTING</h3>

<p>
Before the reducer starts processing the partition data, the records are sorted based on their keys.
</p>

<p>
Sorting ensures that identical keys are placed together, which makes grouping and reduction easier.
</p>

<h3>6. REDUCER PHASE</h3>

<p>
The reducer receives a key along with all the values associated with that key.
</p>

<pre>
Anirudh Ravichander → [1, 1, 1, 1, 1]
</pre>

<p>
The reducer calculates:
</p>

<pre>
1 + 1 + 1 + 1 + 1 = 5
</pre>

<p>
The final result becomes:
</p>

<pre>
Anirudh Ravichander 5
</pre>

<p>
The same process is performed for all music directors.
</p>

<h3>7. FINAL OUTPUT</h3>

<p>
After all reducer processes complete their execution, the final aggregated results are generated.
</p>

<pre>
Anirudh Ravichander    5
Santhosh Narayanan     3
Thaman S               4
Yuvan Shankar Raja     6
</pre>

<p>
The final results can be stored inside:
</p>

<pre>
output/
└── final_output.txt
</pre>

<hr>

<h2>PROJECT STRUCTURE</h2>

<pre>
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
</pre>

<hr>

<h2>FILE DESCRIPTION</h2>

<table>
<tr>
<th>FILE / DIRECTORY</th>
<th>DESCRIPTION</th>
</tr>
<tr>
<td><code>main.py</code></td>
<td>Controls and coordinates the complete MapReduce workflow.</td>
</tr>
<tr>
<td><code>mapper.py</code></td>
<td>Converts input records into intermediate key-value pairs.</td>
</tr>
<tr>
<td><code>partitioner.py</code></td>
<td>Implements custom hash-based partitioning.</td>
</tr>
<tr>
<td><code>reducer.py</code></td>
<td>Aggregates values associated with each music director.</td>
</tr>
<tr>
<td><code>input.txt</code></td>
<td>Contains Tamil movie song records.</td>
</tr>
<tr>
<td><code>intermediate/</code></td>
<td>Stores intermediate partition files.</td>
</tr>
<tr>
<td><code>output/</code></td>
<td>Stores final aggregated results.</td>
</tr>
<tr>
<td><code>README.md</code></td>
<td>Contains project documentation.</td>
</tr>
</table>

<hr>

<h2>TECHNOLOGY STACK</h2>

<table>
<tr>
<th>TECHNOLOGY</th>
<th>USAGE</th>
</tr>
<tr>
<td>Python</td>
<td>Programming Language</td>
</tr>
<tr>
<td>Multiprocessing</td>
<td>Parallel Processing</td>
</tr>
<tr>
<td>MapReduce</td>
<td>Processing Model</td>
</tr>
<tr>
<td>Custom Hash Partitioning</td>
<td>Data Partitioning</td>
</tr>
<tr>
<td>Local File System</td>
<td>Intermediate and Final Storage</td>
</tr>
<tr>
<td>Visual Studio Code</td>
<td>Development Environment</td>
</tr>
<tr>
<td>Git</td>
<td>Version Control</td>
</tr>
<tr>
<td>GitHub</td>
<td>Repository Hosting</td>
</tr>
</table>

<hr>

<h2>KEY FEATURES</h2>

<h3>PARALLEL MAPPER PROCESSING</h3>

<p>
Multiple mapper processes can process different input chunks independently.
</p>

<h3>CUSTOM HASH PARTITIONING</h3>

<p>
The system uses:
</p>

<pre>
hash(key) % number_of_reducers
</pre>

<p>
to distribute intermediate data among reducers.
</p>

<h3>INTERMEDIATE DISK STORAGE</h3>

<p>
The intermediate results are stored as partition files before the reduction stage.
</p>

<h3>SORTING</h3>

<p>
Partitioned records are sorted before reduction so that identical keys can be grouped efficiently.
</p>

<h3>PARALLEL REDUCER PROCESSING</h3>

<p>
Multiple reducers process different partitions independently.
</p>

<h3>DATA AGGREGATION</h3>

<p>
The reducer calculates the total number of songs associated with each music director.
</p>

<h3>MODULAR ARCHITECTURE</h3>

<p>
The project separates the mapper, partitioner, reducer, and main controller into individual Python files.
</p>

<hr>

<h2>REQUIREMENT MAPPING</h2>

<table>
<tr>
<th>REQUIREMENT</th>
<th>IMPLEMENTATION</th>
</tr>
<tr>
<td>MapReduce Engine</td>
<td><code>main.py</code></td>
</tr>
<tr>
<td>Input Splitting</td>
<td>Input records divided into chunks</td>
</tr>
<tr>
<td>Parallel Mapper Processes</td>
<td>Python multiprocessing</td>
</tr>
<tr>
<td>Mapper Logic</td>
<td><code>mapper.py</code></td>
</tr>
<tr>
<td>Intermediate Key-Value Pairs</td>
<td><code>(Music Director, 1)</code></td>
</tr>
<tr>
<td>Custom Hash Partitioning</td>
<td><code>partitioner.py</code></td>
</tr>
<tr>
<td>Partition Formula</td>
<td><code>hash(key) % number_of_reducers</code></td>
</tr>
<tr>
<td>Intermediate Storage</td>
<td><code>intermediate/partition_*.txt</code></td>
</tr>
<tr>
<td>Sorting</td>
<td>Partition records sorted by key</td>
</tr>
<tr>
<td>Parallel Reducer Processes</td>
<td>Python multiprocessing</td>
</tr>
<tr>
<td>Aggregation</td>
<td><code>reducer.py</code></td>
</tr>
<tr>
<td>Final Results</td>
<td><code>output/final_output.txt</code></td>
</tr>
</table>

<hr>

<h2>HOW TO RUN</h2>

<h3>PREREQUISITES</h3>

<p>
Install <strong>Python 3.x</strong> on your system.
</p>

<p>Check the Python version:</p>

<pre>
python --version
</pre>

<h3>STEP 1: NAVIGATE TO THE PROJECT DIRECTORY</h3>

<pre>
cd WEEK-2/Spotify_Tamil_Song_MapReduce
</pre>

<h3>STEP 2: RUN THE MAPREDUCE ENGINE</h3>

<pre>
python main.py
</pre>

<p>The program performs:</p>

<pre>
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
</pre>

<h3>STEP 3: CHECK THE OUTPUT</h3>

<p>The final output can be viewed in:</p>

<pre>
output/final_output.txt
</pre>

<p>The intermediate partition files can be viewed in:</p>

<pre>
intermediate/
</pre>

<hr>

<h2>SAMPLE OUTPUT</h2>

<pre>
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
</pre>

<p>
<strong>Note:</strong> The exact output depends on the records available in <code>input.txt</code>.
</p>

<hr>

<h2>LEARNING OUTCOMES</h2>

<ul>
<li>MapReduce programming model.</li>
<li>Parallel data processing.</li>
<li>Python multiprocessing.</li>
<li>Input splitting.</li>
<li>Mapper and reducer architecture.</li>
<li>Intermediate key-value processing.</li>
<li>Custom hash partitioning.</li>
<li>Local disk-based intermediate storage.</li>
<li>Sorting and grouping.</li>
<li>Data aggregation.</li>
<li>Distributed processing concepts.</li>
<li>Modular Python application development.</li>
</ul>

<hr>

<h2>FUTURE ENHANCEMENTS</h2>

<ul>
<li>Support for larger Tamil movie song datasets.</li>
<li>Dynamic configuration of mapper and reducer counts.</li>
<li>Artist-wise song analysis.</li>
<li>Movie-wise song analysis.</li>
<li>Year-wise song analysis.</li>
<li>Genre-based song analysis.</li>
<li>Song popularity analysis.</li>
<li>Rating-based analytics.</li>
<li>Interactive data visualization dashboards.</li>
<li>Integration with real Spotify datasets or APIs.</li>
<li>Distributed execution across multiple systems.</li>
<li>Integration with Apache Hadoop or Apache Spark.</li>
</ul>

<hr>

<h2>CONCLUSION</h2>

<p>
The <strong>Spotify Tamil Movie Song Analytics Using Distributed MapReduce Engine</strong> project demonstrates the implementation of a simplified MapReduce framework from scratch using Python.
</p>

<p>
The system follows the complete MapReduce data-processing pipeline, beginning with <strong>input splitting and parallel mapping</strong>, followed by <strong>intermediate key-value generation, custom hash partitioning, local disk storage, sorting, and parallel reduction</strong>.
</p>

<p>
The project demonstrates how large datasets can be divided into smaller processing tasks and handled concurrently using independent mapper and reducer processes. By applying the MapReduce model to Tamil movie song data, the system efficiently aggregates song information based on music directors.
</p>

<p>
Overall, this project provides practical knowledge of <strong>parallel processing, distributed computing concepts, MapReduce architecture, hash partitioning, intermediate data management, sorting, and data aggregation</strong>, forming a strong foundation for understanding modern Big Data processing systems.
</p>

<hr>

<h2>AUTHOR</h2>

<p align="center">
<strong>S. YUVASRI</strong><br>
BACHELOR OF COMPUTER APPLICATIONS (BCA)<br>
KAMARAJ COLLEGE, THOOTHUKUDI, TAMIL NADU
</p>
