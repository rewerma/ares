CREATE TABLE test1 (
    id NUMBER(10,0),
    name VARCHAR,
    c_time TIMESTAMP
)
WITH (
    'connector' = 'FileSftp',
    'host' = '127.0.0.1',
    'port' = '22',
    'user' = 'root',
    'password' = '123456',
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
    'connector' = 'FileSftp',
    'host' = '127.0.0.1',
    'port' = '22',
    'user' = 'root',
    'password' = '123456',
    'tmp_path' = '/ares/tmp',
    'path' = '/ares/data2',
    'file_format_type'='text',
    'field_delimiter' = ',',
    'type' = 'sink,source'
);

truncate table test2;
select * from test2;
insert into test2 select * from test1;
select * from test2;