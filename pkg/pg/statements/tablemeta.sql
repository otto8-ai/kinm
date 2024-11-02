SELECT coalesce((SELECT max(id) FROM placeholder), 0) AS max_id,
       coalesce((SELECT c.id
                 FROM compaction AS c
                 WHERE c.name = 'placeholder'), 0)    as compaction_id