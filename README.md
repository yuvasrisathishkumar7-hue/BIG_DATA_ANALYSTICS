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

<p align="center">
  <strong>Distributed MapReduce Engine from Scratch with Custom Hash Partitioning</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Language-Python-blue">
  <img src="https://img.shields.io/badge/Processing-MapReduce-orange">
  <img src="https://img.shields.io/badge/Parallelism-Multiprocessing-green">
  <img src="https://img.shields.io/badge/Domain-Big%20Data%20Analytics-purple">
  <img src="https://img.shields.io/badge/Status-Completed-success">
</p>

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

<p>
<strong>Primary Analytical Task:</strong>
</p>

<blockquote>
Calculate the total number of songs associated with each music director.
</blockquote>

<p>
<strong>Current Dataset:</strong> 20 song records
</p>

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
                              ▼
                   7 MAPPER CHUNKS CREATED
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

<p>
In the current execution, the input data was divided into:
</p>

<pre>
Number of Mapper Chunks: 7
</pre>

<p>
The workflow can be represented as:
</p>

<pre>
Input Dataset
     │
     ├── Chunk 1 → Mapper 1
     ├── Chunk 2 → Mapper 2
     ├── Chunk 3 → Mapper 3
     ├── Chunk 4 → Mapper 4
     ├── Chunk 5 → Mapper 5
     ├── Chunk 6 → Mapper 6
     └── Chunk 7 → Mapper 7
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
The current project execution uses <strong>2 reducer processes</strong>.
</p>

<p>
The main purpose of hash partitioning is to ensure that <strong>all occurrences of the same key are sent to the same reducer</strong>. This is essential for correct aggregation.
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

<p>
For example:
</p>

<pre>
Anirudh Ravichander → [1, 1, 1, 1, 1, 1]
</pre>

<p>
The reducer calculates:
</p>

<pre>
1 + 1 + 1 + 1 + 1 + 1 = 6
</pre>

<p>
The final result becomes:
</p>

<pre>
Anirudh Ravichander 6
</pre>

<p>
The same process is performed for all music directors.
</p>

<h3>7. FINAL OUTPUT</h3>

<p>
After all reducer processes complete their execution, the final aggregated results are generated.
</p>

<p>
The final output is saved to:
</p>

<pre>
output/final_output.txt
</pre>

<hr>

<h2>PROJECT STRUCTURE</h2>

<pre>
Spotify_MapReduce/
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
<td>Contains Tamil movie song records used as input data.</td>
</tr>

<tr>
<td><code>intermediate/</code></td>
<td>Stores intermediate partition files generated during the MapReduce process.</td>
</tr>

<tr>
<td><code>output/</code></td>
<td>Stores the final aggregated results.</td>
</tr>

<tr>
<td><code>README.md</code></td>
<td>Contains complete project documentation.</td>
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
<td>Python 3.11.9</td>
<td>Programming Language</td>
</tr>

<tr>
<td>Multiprocessing</td>
<td>Parallel Mapper and Reducer Processing</td>
</tr>

<tr>
<td>MapReduce</td>
<td>Data Processing Model</td>
</tr>

<tr>
<td>Custom Hash Partitioning</td>
<td>Intermediate Data Distribution</td>
</tr>

<tr>
<td>Local File System</td>
<td>Intermediate and Final Data Storage</td>
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
Multiple mapper processes can process different input chunks independently. In the current execution, the dataset was divided into <strong>7 mapper chunks</strong>.
</p>

<h3>CUSTOM HASH PARTITIONING</h3>

<p>
The system uses:
</p>

<pre>
hash(key) % number_of_reducers
</pre>

<p>
to distribute intermediate data among the reducer processes.
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
Multiple reducers process different partitions independently. The current implementation uses <strong>2 reducer processes</strong>.
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
<td>Input records divided into 7 mapper chunks</td>
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
<td>Number of Reducers</td>
<td>2</td>
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

<p>
The project was successfully executed using:
</p>

<pre>
Python 3.11.9
</pre>

<p>
Check the Python version:
</p>

<pre>
python --version
</pre>

<h3>STEP 1: NAVIGATE TO THE PROJECT DIRECTORY</h3>

<pre>
cd D:\BIG DATA\Spotify_MapReduce
</pre>

<h3>STEP 2: RUN THE MAPREDUCE ENGINE</h3>

<pre>
python main.py
</pre>

<p>The program performs the following operations:</p>

<pre>
Input Reading
      ↓
Input Splitting
      ↓
7 Mapper Chunks Created
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

<p>
The final output can be viewed in:
</p>

<pre>
output/final_output.txt
</pre>

<p>
The intermediate partition files can be viewed in:
</p>

<pre>
intermediate/
</pre>

<hr>

<h2>ACTUAL EXECUTION OUTPUT</h2>

<pre>
============================================================
SPOTIFY TAMIL MOVIE SONG MAPREDUCE ENGINE
============================================================

Input Split Completed
Number of Mapper Chunks: 7

Mapper Processes Completed

Intermediate Key-Value Pairs:
(Anirudh Ravichander, 1)
(Anirudh Ravichander, 1)
(Anirudh Ravichander, 1)
(Anirudh Ravichander, 1)
(Anirudh Ravichander, 1)
(Anirudh Ravichander, 1)
(Thaman S, 1)
(Thaman S, 1)
(Thaman S, 1)
(Yuvan Shankar Raja, 1)
(Yuvan Shankar Raja, 1)
(Yuvan Shankar Raja, 1)
(Santhosh Narayanan, 1)
(Santhosh Narayanan, 1)
(Santhosh Narayanan, 1)
(Justin Prabhakaran, 1)
(Pradeep Kumar, 1)
(Darbuka Siva, 1)
(Nivas K Prasanna, 1)
(Gopi Sundar, 1)

Hash Partitioning Completed
Sorting Completed
Reducer Processes Completed

============================================================
FINAL SPOTIFY TAMIL MOVIE SONG ANALYSIS
============================================================

Music Director: Anirudh Ravichander | Total Songs: 6
Music Director: Darbuka Siva | Total Songs: 1
Music Director: Gopi Sundar | Total Songs: 1
Music Director: Justin Prabhakaran | Total Songs: 1
Music Director: Nivas K Prasanna | Total Songs: 1
Music Director: Pradeep Kumar | Total Songs: 1
Music Director: Santhosh Narayanan | Total Songs: 3
Music Director: Thaman S | Total Songs: 3
Music Director: Yuvan Shankar Raja | Total Songs: 3

Final Output Saved To:
output/final_output.txt

MapReduce Job Completed Successfully!
</pre>

<hr>

<h2>FINAL ANALYSIS SUMMARY</h2>

<table>
<tr>
<th>MUSIC DIRECTOR</th>
<th>TOTAL SONGS</th>
</tr>

<tr>
<td>Anirudh Ravichander</td>
<td>6</td>
</tr>

<tr>
<td>Darbuka Siva</td>
<td>1</td>
</tr>

<tr>
<td>Gopi Sundar</td>
<td>1</td>
</tr>

<tr>
<td>Justin Prabhakaran</td>
<td>1</td>
</tr>

<tr>
<td>Nivas K Prasanna</td>
<td>1</td>
</tr>

<tr>
<td>Pradeep Kumar</td>
<td>1</td>
</tr>

<tr>
<td>Santhosh Narayanan</td>
<td>3</td>
</tr>

<tr>
<td>Thaman S</td>
<td>3</td>
</tr>

<tr>
<td>Yuvan Shankar Raja</td>
<td>3</td>
</tr>

</table>

<h3>EXECUTION SUMMARY</h3>

<table>
<tr>
<th>METRIC</th>
<th>RESULT</th>
</tr>

<tr>
<td>Total Records Processed</td>
<td>20</td>
</tr>

<tr>
<td>Number of Mapper Chunks</td>
<td>7</td>
</tr>

<tr>
<td>Number of Reducers</td>
<td>2</td>
</tr>

<tr>
<td>Unique Music Directors</td>
<td>9</td>
</tr>

<tr>
<td>Highest Song Count</td>
<td>Anirudh Ravichander – 6 Songs</td>
</tr>

<tr>
<td>Final Output File</td>
<td><code>output/final_output.txt</code></td>
</tr>

<tr>
<td>Execution Status</td>
<td>MapReduce Job Completed Successfully</td>
</tr>

</table>

<hr>

<h2>LEARNING OUTCOMES</h2>

<ul>
<li>Understanding the MapReduce programming model.</li>
<li>Implementing parallel data processing using Python.</li>
<li>Working with Python multiprocessing.</li>
<li>Understanding input splitting and mapper execution.</li>
<li>Implementing mapper and reducer architecture.</li>
<li>Generating intermediate key-value pairs.</li>
<li>Implementing custom hash partitioning.</li>
<li>Understanding local disk-based intermediate storage.</li>
<li>Performing sorting and grouping of intermediate data.</li>
<li>Aggregating data using reducer processes.</li>
<li>Understanding distributed processing concepts.</li>
<li>Developing a modular Python-based data processing application.</li>
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
The system successfully follows the complete MapReduce data-processing pipeline, beginning with <strong>input splitting and parallel mapping</strong>, followed by <strong>intermediate key-value generation, custom hash partitioning, local disk storage, sorting, and parallel reduction</strong>.
</p>

<p>
In the current execution, <strong>20 Tamil movie song records</strong> were divided into <strong>7 mapper chunks</strong> and processed using parallel mapper processes. The generated intermediate key-value pairs were distributed using custom hash partitioning and processed by <strong>2 reducer processes</strong>.
</p>

<p>
The final analysis successfully identified the total number of songs associated with <strong>9 different music directors</strong>. Among them, <strong>Anirudh Ravichander</strong> had the highest number of songs in the current dataset, with a total of <strong>6 songs</strong>.
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

<hr>

<p align="center">
<strong>MAPREDUCE  COMPLETED SUCCESSFULLY</strong>
</p>
