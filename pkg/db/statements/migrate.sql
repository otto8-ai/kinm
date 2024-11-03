CREATE TABLE IF NOT EXISTS placeholder
(
    id          INTEGER PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    namespace   VARCHAR(255) NOT NULL,
    previous_id INTEGER UNIQUE,
    uid         VARCHAR(255) NOT NULL,
    created     INTEGER,
    deleted     INTEGER       DEFAULT 0 NOT NULL,
    value       TEXT NOT NULL DEFAULT '',
    CONSTRAINT placeholder_unique_name_namespace_created UNIQUE (name, namespace, created)
);

CREATE TABLE IF NOT EXISTS compaction
(
    name VARCHAR(255) NOT NULL UNIQUE,
    id   INTEGER
);
