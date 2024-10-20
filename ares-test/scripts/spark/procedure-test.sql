CREATE PROCEDURE test(p1 IN INT, p2 IN NUMBER) AS
    a VARCHAR := 'test';
    b INT := 1;
    c TIMESTAMP := '2021-01-01 12:23:34.567';
    d NUMBER(10, 2) := 1.124;
BEGIN
    PUT_LINE(d);
    WHILE b <= p1 LOOP
        PUT_LINE('Current index: '||b);
        IF b > 2 THEN
            PUT_LINE('Exit while loop!');
            EXIT;
        END IF;
        b := b + 1;
    END LOOP;
END;

CALL test(5, 3.14);


CREATE PROCEDURE test2(p1 IN INT, p2 IN NUMBER, p3 OUT VARCHAR) AS
BEGIN
    p3 := (p1 * p2) || '_';
END;

DECLARE
    v1 VARCHAR;
BEGIN
    test2(2, 3.14, v1);
    put_line('Result: '||v1);
END;