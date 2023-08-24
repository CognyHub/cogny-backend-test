DROP VIEW if exists ${schema:raw}.population_total CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.population_total AS
	SELECT SUM((doc_record->>'Population')::INT) AS total_population 
    FROM ${schema:raw}.api_data
    WHERE (doc_record->>'ID Year')::INT IN (2020, 2019, 2018);

