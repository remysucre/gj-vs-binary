COPY (SELECT COUNT(*) FROM movie_link GROUP BY linked_movie_id) TO './tables/movie_link_linked_movie_id.csv' (HEADER, DELIMITER ',');