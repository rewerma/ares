CREATE TABLE test1
USING FileLocal
OPTIONS (
    'path' = '/Users/rewerma/Develop/ares/data',
    'file_format_type'='text',
    'delimiter' = ',',
    'schema' = '{"columns":[{"name":"id","type":"decimal(10,0)"},{"name":"name","type":"string"},{"name":"c_time","type":"timestamp"}]}',
    'type' = 'source'
);

CREATE TABLE test2
USING FileLocal
OPTIONS (
    'path' = '/Users/rewerma/Develop/ares/data2',
    'file_format_type'='text',
    'delimiter' = ',',
    'field_delimiter' = ',',
    'schema' = '{"columns":[{"name":"id","type":"decimal(10,0)"},{"name":"name","type":"string"},{"name":"c_time","type":"timestamp"}]}',
    'type' = 'sink,source'
);

truncate table test2;
insert into test2 select id+1,name,c_time from test1;
-- reload('test2');
-- update test2 a, test1 b set a.name = b.name where a.id = b.id;
select * from test2;
