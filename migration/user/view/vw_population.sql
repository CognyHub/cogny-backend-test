DROP VIEW if exists ${schema:raw}.vw_population CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.vw_population AS
	(WITH records AS (
			SELECT jsonb_array_elements(doc_record) AS record
			FROM ${schema:raw}.api_data
	)
	SELECT SUM((record->>'Population')::integer) AS total_population
	FROM records
	WHERE record->>'Year' IN ('2018', '2019', '2020'))
;

