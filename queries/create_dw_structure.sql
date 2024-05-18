DROP SCHEMA IF EXISTS rating_analysis CASCADE;

CREATE SCHEMA rating_analysis;

DROP TABLE IF EXISTS time;
CREATE TABLE time (
    date DATE NOT NULL PRIMARY KEY,
    day INT NOT NULL CHECK(day>=1 AND day<=31),
    month INT NOT NULL CHECK(month>=1 AND month<=12),
    year INT NOT NULL
);

DROP TABLE IF EXISTS movies;
CREATE TABLE movies(
    id VARCHAR(100) NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    launch_year INT,
    hosted_at VARCHAR(100) NOT NULL
);

DROP TABLE IF EXISTS rating;
CREATE TABLE rating(
    rating_date DATE NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    movie_id VARCHAR(100) NOT NULL,
    rating FLOAT,
    FOREIGN KEY (rating_date) REFERENCES time(date),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);