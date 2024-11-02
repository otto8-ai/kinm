CREATE TABLE IF NOT EXISTS placeholder
(
    id          BIGINT PRIMARY KEY UNIQUE,
    name        VARCHAR(255) NOT NULL,
    namespace   VARCHAR(255) NOT NULL,
    previous_id BIGINT UNIQUE,
    uid         VARCHAR(255) NOT NULL,
    created     BOOLEAN      NULL,
    deleted     BOOLEAN               DEFAULT FALSE NOT NULL,
    value       TEXT         NOT NULL DEFAULT '',
    CONSTRAINT placeholder_unique_name_namespace_created UNIQUE (name, namespace, created)
);

CREATE TABLE IF NOT EXISTS compaction
(
    name VARCHAR(255) NOT NULL UNIQUE,
    id   BIGINT
);
