CREATE TABLE IF NOT EXISTS ds_master( 
id BIGINT PRIMARY KEY,
lesson_id text,
collection_id text,
collection_title text,
resource_id text,
resource_title text,
reaction smallint,
views integer);

CREATE INDEX resource_index ON ds_master(resource_id);
