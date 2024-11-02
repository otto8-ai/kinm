INSERT INTO compaction(name, id)
VALUES ('placeholder',
    (SELECT greatest(max(r.id), 1) FROM placeholder AS r))
ON CONFLICT (name) DO UPDATE SET id = EXCLUDED.id;
