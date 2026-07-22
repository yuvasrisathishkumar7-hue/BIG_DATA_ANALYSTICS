def mapper(line):

    data = line.strip().split(",")

    if len(data) != 3:
        return []

    song = data[0].strip()
    music_director = data[1].strip()
    movie = data[2].strip()

    return [(music_director, 1)]