COPY (SELECT COUNT(*) FROM movie_companies GROUP BY company_type_id) TO './tables/movie_companies_company_type_id.csv' (HEADER, DELIMITER ',');