CREATE TABLE test1
WITH (
    'connector' = 'fake',
    'schema' = '{"fields":{"id":"bigint","name":"string","c_time":"timestamp"}}',
    'rows' = '[{"fields":[1, "Eric", "2021-01-01 12:23:34"]},
               {"fields":[2, "Andy", "2022-03-11 11:23:34"]},
               {"fields":[3, "Joker", "2024-11-04 10:23:34"]}]',
    'type' = 'source'
);

select * from test1;