COPY INTO dates
FROM (SELECT
        $1:date::DATE AS date,
        $1:day::INT AS day,
        $1:month::INT AS month,
        $1:year::INT AS year
      FROM @STG_TIME_DIMENSION
      )
FILE_FORMAT = (TYPE = 'PARQUET');


COPY INTO movies
FROM (SELECT
        $1:id::STRING AS id,
        substr($1:name,0,254)::STRING AS name,
        $1:launch_year::INT AS launch_year,
        $1:hosted_at::STRING AS hosted_at
      FROM @STG_MOVIE_DIMENSION
      )
FILE_FORMAT = (TYPE = 'PARQUET');


COPY INTO ratings
FROM (SELECT
        $1:rating_date::date AS rating_date,
        $1:client_id::STRING AS client_id,
        $1:movie_id::STRING AS movie_id,
        $1:rating::FLOAT AS rating
      FROM @STG_FACT_TABLE
      )
FILE_FORMAT = (TYPE = 'PARQUET');