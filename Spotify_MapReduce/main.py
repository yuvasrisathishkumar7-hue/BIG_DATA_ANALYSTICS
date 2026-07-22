import multiprocessing
import os
import shutil

from mapper import mapper
from reducer import reducer
from partitioner import partition


NUMBER_OF_REDUCERS = 2


# -------------------------
# Mapper Process
# -------------------------

def mapper_worker(lines):

    result = []

    for line in lines:

        mapped_values = mapper(line)

        result.extend(mapped_values)

    return result


# -------------------------
# Partitioning
# -------------------------

def create_partitions(mapped_data):

    partitions = {}

    for reducer_id in range(NUMBER_OF_REDUCERS):

        partitions[reducer_id] = []


    for key, value in mapped_data:

        reducer_id = partition(
            key,
            NUMBER_OF_REDUCERS
        )

        partitions[reducer_id].append(
            (key, value)
        )


    os.makedirs(
        "intermediate",
        exist_ok=True
    )


    for reducer_id in range(NUMBER_OF_REDUCERS):

        data = partitions[reducer_id]

        # Sort before writing to disk
        data.sort(
            key=lambda x: x[0]
        )


        filename = (
            f"intermediate/"
            f"partition_{reducer_id}.txt"
        )


        with open(
            filename,
            "w",
            encoding="utf-8"
        ) as file:

            for key, value in data:

                file.write(
                    f"{key}\t{value}\n"
                )


# -------------------------
# Reducer Process
# -------------------------

def reducer_worker(reducer_id):

    filename = (
        f"intermediate/"
        f"partition_{reducer_id}.txt"
    )


    grouped = {}


    if not os.path.exists(filename):

        return []


    with open(
        filename,
        "r",
        encoding="utf-8"
    ) as file:

        for line in file:

            line = line.strip()

            if not line:
                continue


            key, value = line.split(
                "\t"
            )


            value = int(value)


            if key not in grouped:

                grouped[key] = []


            grouped[key].append(
                value
            )


    output = []


    for key in sorted(grouped.keys()):

        output.append(
            reducer(
                key,
                grouped[key]
            )
        )


    return output


# -------------------------
# Main Program
# -------------------------

if __name__ == "__main__":


    print(
        "\n"
        + "=" * 60
    )

    print(
        "SPOTIFY TAMIL MOVIE SONG "
        "MAPREDUCE ENGINE"
    )

    print(
        "=" * 60
    )


    # -------------------------
    # Clean Previous Data
    # -------------------------

    if os.path.exists("intermediate"):

        shutil.rmtree("intermediate")


    if os.path.exists("output"):

        shutil.rmtree("output")


    # -------------------------
    # Read Input File
    # -------------------------

    with open(
        "input.txt",
        "r",
        encoding="utf-8"
    ) as file:

        lines = file.readlines()


    # Remove Header

    lines = lines[1:]


    # -------------------------
    # Split Input
    # -------------------------

    chunks = []

    size = 3


    for i in range(
        0,
        len(lines),
        size
    ):

        chunks.append(
            lines[i:i + size]
        )


    print(
        "\nInput Split Completed"
    )

    print(
        "Number of Mapper Chunks:",
        len(chunks)
    )


    # -------------------------
    # Start Mapper Processes
    # -------------------------

    mapper_pool = multiprocessing.Pool(
        processes=len(chunks)
    )


    mapper_results = mapper_pool.map(
        mapper_worker,
        chunks
    )


    mapper_pool.close()

    mapper_pool.join()


    print(
        "\nMapper Processes Completed"
    )


    # -------------------------
    # Combine Mapper Outputs
    # -------------------------

    intermediate = []


    for result in mapper_results:

        intermediate.extend(result)


    print(
        "\nIntermediate Key-Value Pairs:"
    )


    for key, value in intermediate:

        print(
            f"({key}, {value})"
        )


    # -------------------------
    # Hash Partitioning
    # -------------------------

    create_partitions(
        intermediate
    )


    print(
        "\nHash Partitioning Completed"
    )


    # -------------------------
    # Sorting Completed
    # -------------------------

    print(
        "Sorting Completed"
    )


    # -------------------------
    # Start Reducer Processes
    # -------------------------

    reducer_pool = multiprocessing.Pool(
        NUMBER_OF_REDUCERS
    )


    final_output = reducer_pool.map(
        reducer_worker,
        range(NUMBER_OF_REDUCERS)
    )


    reducer_pool.close()

    reducer_pool.join()


    print(
        "Reducer Processes Completed"
    )


    # -------------------------
    # Combine Final Results
    # -------------------------

    results = []


    for reducer_result in final_output:

        results.extend(
            reducer_result
        )


    # Sort Final Results

    results.sort(
        key=lambda x: x[0]
    )


    # -------------------------
    # Create Output Directory
    # -------------------------

    os.makedirs(
        "output",
        exist_ok=True
    )


    # -------------------------
    # Write Final Output
    # -------------------------

    output_file = (
        "output/final_output.txt"
    )


    with open(
        output_file,
        "w",
        encoding="utf-8"
    ) as file:

        for key, value in results:

            file.write(
                f"{key}\t{value}\n"
            )


    # -------------------------
    # Display Final Output
    # -------------------------

    print(
        "\n"
        + "=" * 60
    )

    print(
        "FINAL SPOTIFY TAMIL MOVIE "
        "SONG ANALYSIS"
    )

    print(
        "=" * 60
    )


    for key, value in results:

        print(
            f"Music Director: "
            f"{key} | "
            f"Total Songs: {value}"
        )


    print(
        "\nFinal Output Saved To:"
    )

    print(
        output_file
    )


    print(
        "\nMapReduce Job Completed Successfully!"
    )