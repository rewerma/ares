CREATE TABLE test1
USING FileSftp
OPTIONS (
    'host' = '127.0.0.1',
    'port' = '22',
    'user' = 'root',
    'password' = '123456',
    'path' = '/ares/data',
    'file_format_type'='text',
    'delimiter' = ',',
    'schema' = '{"columns":[{"name":"id","type":"decimal(10,0)"},{"name":"name","type":"string"},{"name":"c_time","type":"timestamp"}]}',
    'type' = 'source'
);

CREATE TABLE test2
USING FileSftp
OPTIONS (
    'host' = '127.0.0.1',
    'port' = '22',
    'user' = 'root',
    'password' = '123456',
    'tmp_path' = '/ares/tmp',
    'path' = '/ares/data2',
    'file_format_type'='text',
    'field_delimiter' = ',',
    'schema' = '{"columns":[{"name":"id","type":"decimal(10,0)"},{"name":"name","type":"string"},{"name":"c_time","type":"timestamp"}]}',
    'type' = 'sink,source'
);

truncate table test2;
select * from test2;
insert into test2 select * from test1;
select * from test2;
