CREATE TABLE dates (
    date DATE NOT NULL PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
);

CREATE TABLE movies(
    id VARCHAR(100) NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    launch_year INT,
    hosted_at VARCHAR(100) NOT NULL
);

CREATE TABLE rating(
    rating_date DATE NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    movie_id VARCHAR(100) NOT NULL,
    rating FLOAT,
    FOREIGN KEY (rating_date) REFERENCES dates(date),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);