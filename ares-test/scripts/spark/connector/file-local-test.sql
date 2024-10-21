CREATE TABLE test1 (
    id NUMBER(10,0),
    name VARCHAR,
    c_time TIMESTAMP
)
WITH (
    'connector' = 'FileLocal',
    'path' = '/ares/data',
    'file_format_type'='text',
    'delimiter' = ',',
    'type' = 'source'
);

CREATE TABLE test2
(
    id NUMBER(10,0),
    name VARCHAR,
    c_time TIMESTAMP
)
WITH (
    'connector' = 'FileLocal',
    'path' = '/ares/data2',
    'file_format_type'='text',
    'delimiter' = ',',
    'field_delimiter' = ',',
    'type' = 'sink,source'
);

truncate table test2;
insert into test2 select * from test1;
-- reload('test2');
-- update test2 a, test1 b set a.name = b.name where a.id = b.id;
select * from test2;
