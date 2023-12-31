/* The meta schema oversees database automation and is not exposed to the user */
CREATE SCHEMA IF NOT EXISTS meta;

/* Enumerate all symbols per schemata, which correspond to tables */
CREATE TABLE IF NOT EXISTS meta.symbols (
	symbol varchar PRIMARY KEY,
	figi varchar
);

/* Enumerate all fields per symbol */
CREATE TABLE IF NOT EXISTS meta.fields (
	field varchar PRIMARY KEY,
	data_type varchar NOT NULL
);

/* Each vendor maps to a Python implementation */
CREATE TABLE IF NOT EXISTS meta.vendors (
	vendor varchar PRIMARY KEY,
	rate_limit int,
	active bool NOT NULL DEFAULT true
);

/* Endpoints correspond to individual functions */
CREATE TABLE IF NOT EXISTS meta.endpoints (
	endpoint varchar NOT NULL,
	vendor varchar REFERENCES meta.vendors,
	rate_limit int,
	signature json,
	PRIMARY KEY (endpoint, vendor)
);

/* Enumerate all automated function calls */
CREATE TABLE IF NOT EXISTS meta.calls (
	call_id serial PRIMARY KEY,
	vendor varchar,
	endpoint varchar,
	field varchar REFERENCES meta.fields,
	symbol varchar REFERENCES meta.symbols,
	args json,
	FOREIGN KEY (endpoint, vendor) REFERENCES meta.endpoints(endpoint, vendor)
);
