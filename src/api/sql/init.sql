/* The meta schema oversees database automation and is not exposed to the user */
CREATE SCHEMA IF NOT EXISTS _meta;

/* Each vendor maps to a Python implementation */
CREATE TABLE IF NOT EXISTS _meta.vendors (
	vendor varchar PRIMARY KEY,
	rate_limit int,
	active bool NOT NULL DEFAULT true
);

/* Enumerate all symbols per schemata, which correspond to tables */
CREATE TABLE IF NOT EXISTS _meta.symbols (
	symbol varchar NOT NULL,
	vendor varchar REFERENCES _meta.vendors,
	figi varchar
);

/* Enumerate all fields per symbol */
CREATE TABLE IF NOT EXISTS _meta.fields (
	field varchar NOT NULL,
	vendor varchar REFERENCES _meta.vendors,
	data_type varchar NOT NULL
);
