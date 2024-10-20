SET ares.pl.trace.enabled=true;

SELECT 1;
-- CREATE FUNCTION test(p1 INT) RETURN INT AS
--   v1 INT := 1;
-- BEGIN
--     RETURN p1 + 1 + v1;
-- END;
--
-- put_Line(test(1));

-- CREATE PROCEDURE abc(p1 IN INT, p2 out VARCHAR) AS
--     c1 DOUBLE := 3.1415926;
--     c DECIMAL(10, 2);
-- BEGIN
--     c := c1 + 1.234566;
--     put_line(c);
--     p2 := 'Hello World';
-- END;

-- DECLARE
--     b VARCHAR := 'Hello';
-- BEGIN
--     abc(123, b);
--     put_line(b);
-- --     c := c1 + 1.234566;
-- --     put_line(c);
-- --     IF bl THEN
-- --         PUT_LINE('Exit while loop!');
-- --     END IF;
-- END;

-- DECLARE
-- --     a BIGINT := 1;
-- --     b VARCHAR := 'Hello World';
--     c1 DOUBLE := 3.1415926;
--     c DECIMAL(10, 2);
-- --     d DATE := '2022-01-01';
-- --     e TIMESTAMP := '2022-01-01 12:23:34';
-- --     f BINARY := 'abc';
--     bl boolean := 1;
-- BEGIN
--     c := c1 + 1.234566;
--     put_line(c);
--     IF bl THEN
--         PUT_LINE('Exit while loop!');
--     END IF;
-- END;


-- CREATE FUNCTION test5(p1 DECIMAL(10, 4)) RETURN DECIMAL(10, 4) AS
-- BEGIN
--     RETURN p1 + 1.23231;
-- END;
--
-- put_line(test5(1.1234567));