

-- Quantos filmes estão disponíveis na Amazon?
SELECT COUNT(*) as movies_available_on_amazon from movies where hosted_at = 'AMAZON';



-- Quantos filmes estão disponíveis na Netflix?
SELECT COUNT(*) as movies_available_on_netflix from movies where hosted_at = 'NETFLIX';
-- Dos filmes disponíveis na Amazon, quantos % estão disponíveis na Netflix?
WITH
    movies_netflix as (SELECT * FROM MOVIES WHERE hosted_at='NETFLIX'),
    movies_amazon as (SELECT *  FROM MOVIES WHERE hosted_at='AMAZON'),
    amazon_count as (SELECT COUNT(*) AS num_movies FROM MOVIES WHERE hosted_at='AMAZON'),
    both_count as ( SELECT count(*) as num_movies
                    FROM movies_netflix,movies_amazon
                    WHERE movies_netflix.name = movies_amazon.name )
SELECT both_count.num_movies/amazon_count.num_movies* 100 as percentagem from amazon_count,both_count;                 
--Quão próxima é a média das notas dos filmes da Amazon em relação à da Netflix?
WITH ratings_per_plataform AS(
    SELECT m.hosted_at as plataform ,r.rating as ratings
    FROM ratings as r
    INNER JOIN movies as m
    ON r.movie_id = m.id
),avg_ratings_netflix AS ( SELECT AVG(ratings) as value from ratings_per_plataform where plataform = 'NETFLIX' ),
avg_ratings_amazon AS ( SELECT AVG(ratings) as value from ratings_per_plataform where plataform = 'AMAZON' )
select ROUND(avg_ratings_amazon.value - avg_ratings_netflix.value, 2) as avg_evaluation_diff_plataforms
from avg_ratings_netflix,avg_ratings_amazon;


-- Qual ano de lançamento possui mais filmes na Netflix?

SELECT LAUNCH_YEAR AS YEAR_WITH_MOST_LAUNCHES
FROM MOVIES 
GROUP BY LAUNCH_YEAR
ORDER BY COUNT(LAUNCH_YEAR) DESC
LIMIT 1;

-- Quais filmes que não estão disponíveis no catálogo da Amazon foram *melhor avaliados?
SELECT DISTINCT m.name as movie_name, r.rating as rating
FROM movies as m
INNER JOIN ratings as r
ON m.id = r.movie_id
WHERE m.name not in ( select name from movies where hosted_at = 'AMAZON') AND r.rating IN(4,5);

-- Quais filmes que não estão disponíveis no catálogo da Netflix foram *melhor avaliados?
SELECT DISTINCT m.name as movie_name, r.rating as rating
FROM movies as m
INNER JOIN ratings as r
ON m.id = r.movie_id
WHERE m.name not in ( select name from movies where hosted_at = 'NETFLIX') AND r.rating IN(4,5);

-- Quais os 10 filmes que possuem mais avaliações nas plataformas?
SELECT m.name,COUNT(r.movie_id) as movie_evaluations
FROM ratings AS r
INNER JOIN movies AS m
ON  r.movie_id = m.id
GROUP BY m.name
ORDER BY count(r.movie_id) DESC
LIMIT 10;

-- Quais são os 5 clientes que mais avaliaram filmes na Amazon ?
SELECT client_id,COUNT(client_id) as evaluations
FROM ratings
WHERE client_id like('AMA%') -- clients from amazon has id starting with 'AMA'
GROUP BY client_id
ORDER BY COUNT(client_id) DESC
LIMIT 5;

--  quantos produtos diferentes foram avaliados pelos 5 clientes que mais avaliaram na amazon?
select COUNT( DISTINCT movie_id) as evaluted_products_top_evaluators
FROM ratings
WHERE client_id IN( SELECT client_id
                 FROM ratings
                WHERE client_id like('AMA%') -- clients from amazon has id starting with 'AMA'
                GROUP BY client_id
                ORDER BY COUNT(client_id) DESC
                LIMIT 5
            );

-- Quais são os 5 clientes que mais avaliaram filmes na NETFLIX ?
SELECT client_id,COUNT(client_id) as evaluations
FROM ratings
WHERE client_id like('NET%') -- clients from netflix has id starting with 'NET'
GROUP BY client_id
ORDER BY COUNT(client_id) DESC
LIMIT 5;

--  quantos produtos diferentes foram avaliados pelos 5 clientes que mais avaliaram na netflix?
SELECT COUNT( DISTINCT movie_id) as evaluted_products_top_evaluators
FROM ratings
WHERE client_id IN( SELECT client_id
                    FROM ratings
                    WHERE client_id like('NET%') -- clients from netflix has id starting with 'NET'
                    GROUP BY client_id
                    ORDER BY COUNT(client_id) DESC
                    LIMIT 5
            );
-- Quantos filmes foram avaliados na data de avaliação mais recente na Amazon?

SELECT COUNT(movie_id) as most_recent_evaluations
FROM ratings
where rating_date  = ( SELECT MAX(rating_date) FROM ratings WHERE client_id LIKE('AMA%')
                    );


-- Quais os 10 filmes mais bem avaliados na data mais recente da amazon?
SELECT m.name AS top_10_rating FROM ratings AS r INNER JOIN movies AS m ON r.movie_id = m.id
WHERE rating_date  =  (  SELECT MAX(rating_date) FROM ratings WHERE client_id LIKE('AMA%') )
GROUP BY m.name
ORDER BY AVG(r.rating) DESC
LIMIT 10;


-- Quantos filmes foram avaliados na data de avaliação mais recente na Netflix?
SELECT COUNT(movie_id) as most_recent_evaluations
FROM ratings
WHERE rating_date  = ( SELECT  Max(rating_date)
                        FROM ratings
                        WHERE client_id LIKE('NET%')
                    );


-- Quais os 10 filmes mais bem avaliados na data mais recente da Netflix?
SELECT m.name AS top_10_rating FROM ratings AS r INNER JOIN movies AS m ON r.movie_id = m.id
WHERE rating_date  =  ( SELECT MAX(rating_date) FROM ratings WHERE client_id LIKE('NET%') ) 
GROUP BY m.name
ORDER BY AVG(r.rating) DESC
LIMIT 10;
