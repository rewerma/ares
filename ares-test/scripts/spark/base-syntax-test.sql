CREATE TABLE test1
WITH (
    'connector' = 'fake',
    'schema' = '{"fields":{"id":"bigint","name":"string","c_time":"timestamp"}}',
    'rows' = '[{"fields":[1, "Eric", "2021-01-01 12:23:34"]},
               {"fields":[2, "Andy", "2022-03-11 11:23:34"]},
               {"fields":[3, "Joker", "2024-11-04 10:23:34"]}]',
    'type' = 'source'
);

DECLARE
    i INT := 0;
    e INT := 5;
BEGIN
    WHILE i < 5 LOOP
        IF i > 2 THEN
            EXIT;
        END IF;
        PUT_LINE('INDEX: ' || i);
        i := i + 1;
    END LOOP;

    FOR j IN 1..e LOOP
        IF j = 3 THEN
            EXIT;
        END IF;
        PUT_LINE('INDEX: ' || j);
    END LOOP;

    FOR cur IN (select * from test1) LOOP
        println(cur.id||' '||cur.name||' '||cur.c_time);
    END LOOP;
END;