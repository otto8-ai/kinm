WITH to_delete AS (SELECT prev.id AS id
                   FROM placeholder AS prev
                            JOIN placeholder AS cur ON (
                       (prev.id = cur.previous_id OR
                        (prev.id = cur.id AND (cur.deleted is true)))
                           AND cur.id <= coalesce(
                               (SELECT id AS id
                                FROM compaction
                                WHERE name = 'placeholder')
                           , 0)
                       )
                   LIMIT 500)

DELETE
FROM placeholder AS r
    USING to_delete
WHERE r.id = to_delete.id;