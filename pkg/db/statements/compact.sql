WITH to_delete AS (SELECT prev.id AS id
                   FROM placeholder AS prev
                            JOIN placeholder AS cur ON (
                       (prev.id = cur.previous_id OR
                        (prev.id = cur.id AND (cur.deleted = 1)))
                           AND cur.id <= coalesce(
                               (SELECT id AS id
                                FROM compaction
                                WHERE name = 'placeholder')
                           , 0)
                       )
                   LIMIT 500)

DELETE
FROM placeholder
WHERE id IN (SELECT id FROM to_delete);
