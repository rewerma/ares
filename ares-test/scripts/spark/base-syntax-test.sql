CREATE TABLE test1
USING fake
OPTIONS (
    'schema' = '{"fields":{"id":"bigint","name":"string","c_time":"timestamp"}}',
    'rows' = '[{"fields":[1, "Eric", "2021-01-01 12:23:34"]},
               {"fields":[2, "Andy", "2022-03-11 11:23:34"]},
               {"fields":[3, "Joker", "2024-11-04 10:23:34"]}]',
    'type' = 'source'
);

def i = 0;
def e = 5;
while i < 5
    if i > 2
        break;
    end
    PUT_LINE('INDEX: ' || i);
    i = i + 1;
end

for j in 1 .. e
    if j = 3
        break;
    end
    PUT_LINE('INDEX: ' || j);
end

for cur in (select * from test1)
    println(cur.id||' '||cur.name||' '||cur.c_time);
end
