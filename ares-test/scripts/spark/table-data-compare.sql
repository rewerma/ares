-- =============================================================================
-- 两库表数据比对脚本（支持证件号 zjh 重复）
--
-- 比对策略：
--   1. 以完整行 (xm, zjh, hzxm, card_id, gx) 为比对单元（多重集），
--      同一 zjh 下可存在多条不同记录，按全字段分组统计条数。
--   2. zjh 维度汇总：证件号仅在一侧存在，或两侧该 zjh 的总条数不一致。
--
-- 差异类型：
--   ONLY_IN_A      - 该行签名仅在库A出现
--   ONLY_IN_B      - 该行签名仅在库B出现
--   COUNT_DIFF     - 行签名两侧都有，但出现次数不一致
--   ONLY_ZJH_IN_A  - 证件号仅在库A出现（该 zjh 下无任何行能在B侧匹配）
--   ONLY_ZJH_IN_B  - 证件号仅在库B出现
--   ZJH_CNT_DIFF   - 证件号两侧都有，但该 zjh 下总记录条数不一致
--
-- 执行示例：
--   ./bin/ares-local-starter.sh --sql /path/to/table-data-compare.sql
-- =============================================================================

-- ---------- 库 A 数据源配置 ----------
-- StarRocks/SelectDB/Doris 默认 query_timeout=300s，全表拉取易超时。
-- 可通过 query_timeout_sec 在 JDBC 连接建立后自动 SET query_timeout（单位：秒）。
SET datasource.db_a.connector=mysql;
SET datasource.db_a.url=jdbc:mysql://127.0.0.1:3306/database_a?useSSL=false&characterEncoding=utf8;
SET datasource.db_a.driver=com.mysql.cj.jdbc.Driver;
SET datasource.db_a.user=root;
SET datasource.db_a.password=your_password;
SET datasource.db_a.query_timeout_sec=3600;

-- ---------- 库 B 数据源配置 ----------
SET datasource.db_b.connector=mysql;
SET datasource.db_b.url=jdbc:mysql://127.0.0.1:3306/database_b?useSSL=false&characterEncoding=utf8;
SET datasource.db_b.driver=com.mysql.cj.jdbc.Driver;
SET datasource.db_b.user=root;
SET datasource.db_b.password=your_password;
SET datasource.db_b.query_timeout_sec=3600;

CREATE TABLE tbl_a
WITH (
    'datasource' = 'db_a',
    'table_name' = 'your_table_name',
    'type' = 'source'
);

CREATE TABLE tbl_b
WITH (
    'datasource' = 'db_b',
    'table_name' = 'your_table_name',
    'type' = 'source'
);

CREATE TABLE v_a AS
SELECT xm, zjh, hzxm, card_id, gx FROM tbl_a;

CREATE TABLE v_b AS
SELECT xm, zjh, hzxm, card_id, gx FROM tbl_b;

-- 行签名多重集：全字段分组计数
CREATE TABLE sig_a AS
SELECT xm, zjh, hzxm, card_id, gx, COUNT(*) AS cnt
FROM v_a
GROUP BY xm, zjh, hzxm, card_id, gx;

CREATE TABLE sig_b AS
SELECT xm, zjh, hzxm, card_id, gx, COUNT(*) AS cnt
FROM v_b
GROUP BY xm, zjh, hzxm, card_id, gx;

-- 证件号维度条数
CREATE TABLE zjh_cnt_a AS
SELECT zjh, COUNT(*) AS cnt
FROM v_a
GROUP BY zjh;

CREATE TABLE zjh_cnt_b AS
SELECT zjh, COUNT(*) AS cnt
FROM v_b
GROUP BY zjh;

DECLARE
    v_cnt_a          INT := 0;
    v_cnt_b          INT := 0;
    v_zjh_a          INT := 0;
    v_zjh_b          INT := 0;
    v_sig_match      INT := 0;
    v_only_in_a      INT := 0;
    v_only_in_b      INT := 0;
    v_count_diff     INT := 0;
    v_only_zjh_a     INT := 0;
    v_only_zjh_b     INT := 0;
    v_zjh_cnt_diff   INT := 0;
    v_surplus_a      INT := 0;
    v_surplus_b      INT := 0;
    v_only_rows_a    INT := 0;
    v_only_rows_b    INT := 0;
BEGIN
    SELECT COUNT(*) INTO :v_cnt_a FROM v_a;
    SELECT COUNT(*) INTO :v_cnt_b FROM v_b;
    SELECT COUNT(*) INTO :v_zjh_a FROM zjh_cnt_a;
    SELECT COUNT(*) INTO :v_zjh_b FROM zjh_cnt_b;

    -- 行签名完全一致（多重集相等）
    SELECT COUNT(*) INTO :v_sig_match
    FROM sig_a a
    INNER JOIN sig_b b
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE a.cnt = b.cnt;

    -- 行签名仅在库A
    SELECT COUNT(*) INTO :v_only_in_a
    FROM sig_a a
    LEFT JOIN sig_b b
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE b.zjh IS NULL;

    -- 行签名仅在库B
    SELECT COUNT(*) INTO :v_only_in_b
    FROM sig_b b
    LEFT JOIN sig_a a
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE a.zjh IS NULL;

    -- 行签名两侧都有，但条数不一致
    SELECT COUNT(*) INTO :v_count_diff
    FROM sig_a a
    INNER JOIN sig_b b
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE a.cnt <> b.cnt;

    -- 证件号仅在库A / 库B
    SELECT COUNT(*) INTO :v_only_zjh_a
    FROM zjh_cnt_a a
    LEFT JOIN zjh_cnt_b b ON a.zjh <=> b.zjh
    WHERE b.zjh IS NULL;

    SELECT COUNT(*) INTO :v_only_zjh_b
    FROM zjh_cnt_b b
    LEFT JOIN zjh_cnt_a a ON a.zjh <=> b.zjh
    WHERE a.zjh IS NULL;

    -- 证件号两侧都有，但总条数不一致
    SELECT COUNT(*) INTO :v_zjh_cnt_diff
    FROM zjh_cnt_a a
    INNER JOIN zjh_cnt_b b ON a.zjh <=> b.zjh
    WHERE a.cnt <> b.cnt;

    -- 库A多出来的行实例数（按签名差额累计）
    SELECT COALESCE(SUM(a.cnt - b.cnt), 0) INTO :v_surplus_a
    FROM sig_a a
    INNER JOIN sig_b b
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE a.cnt > b.cnt;

    SELECT COALESCE(SUM(b.cnt - a.cnt), 0) INTO :v_surplus_b
    FROM sig_b b
    INNER JOIN sig_a a
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE b.cnt > a.cnt;

    SELECT COALESCE(SUM(a.cnt), 0) INTO :v_only_rows_a
    FROM sig_a a
    LEFT JOIN sig_b b
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE b.zjh IS NULL;

    SELECT COALESCE(SUM(b.cnt), 0) INTO :v_only_rows_b
    FROM sig_b b
    LEFT JOIN sig_a a
        ON a.xm <=> b.xm
       AND a.zjh <=> b.zjh
       AND a.hzxm <=> b.hzxm
       AND a.card_id <=> b.card_id
       AND a.gx <=> b.gx
    WHERE a.zjh IS NULL;

    PUT_LINE('========== 数据比对汇总（支持 zjh 重复）==========');
    PUT_LINE('库A总记录数: ' || v_cnt_a);
    PUT_LINE('库B总记录数: ' || v_cnt_b);
    PUT_LINE('库A证件号(zjh)数: ' || v_zjh_a);
    PUT_LINE('库B证件号(zjh)数: ' || v_zjh_b);
    PUT_LINE('--- 行级（全字段多重集）---');
    PUT_LINE('完全一致签名组数: ' || v_sig_match);
    PUT_LINE('仅在库A的签名组数: ' || v_only_in_a);
    PUT_LINE('仅在库B的签名组数: ' || v_only_in_b);
    PUT_LINE('仅在库A的行实例数: ' || v_only_rows_a);
    PUT_LINE('仅在库B的行实例数: ' || v_only_rows_b);
    PUT_LINE('签名相同但条数不一致组数: ' || v_count_diff);
    PUT_LINE('签名相同但库A多出行实例数: ' || v_surplus_a);
    PUT_LINE('签名相同但库B多出行实例数: ' || v_surplus_b);
    PUT_LINE('--- 证件号级 ---');
    PUT_LINE('仅在库A的证件号数: ' || v_only_zjh_a);
    PUT_LINE('仅在库B的证件号数: ' || v_only_zjh_b);
    PUT_LINE('证件号两侧都有但总条数不一致数: ' || v_zjh_cnt_diff);
    PUT_LINE('================================================');
END;

-- 明细1: 行签名仅在库A
SELECT
    a.zjh,
    a.xm,
    a.hzxm,
    a.card_id,
    a.gx,
    a.cnt AS cnt_a,
    'ONLY_IN_A' AS diff_type
FROM sig_a a
LEFT JOIN sig_b b
    ON a.xm <=> b.xm
   AND a.zjh <=> b.zjh
   AND a.hzxm <=> b.hzxm
   AND a.card_id <=> b.card_id
   AND a.gx <=> b.gx
WHERE b.zjh IS NULL;

-- 明细2: 行签名仅在库B
SELECT
    b.zjh,
    b.xm,
    b.hzxm,
    b.card_id,
    b.gx,
    b.cnt AS cnt_b,
    'ONLY_IN_B' AS diff_type
FROM sig_b b
LEFT JOIN sig_a a
    ON a.xm <=> b.xm
   AND a.zjh <=> b.zjh
   AND a.hzxm <=> b.hzxm
   AND a.card_id <=> b.card_id
   AND a.gx <=> b.gx
WHERE a.zjh IS NULL;

-- 明细3: 行签名相同但出现次数不一致
SELECT
    a.zjh,
    a.xm,
    a.hzxm,
    a.card_id,
    a.gx,
    a.cnt AS cnt_a,
    b.cnt AS cnt_b,
    a.cnt - b.cnt AS cnt_delta,
    'COUNT_DIFF' AS diff_type
FROM sig_a a
INNER JOIN sig_b b
    ON a.xm <=> b.xm
   AND a.zjh <=> b.zjh
   AND a.hzxm <=> b.hzxm
   AND a.card_id <=> b.card_id
   AND a.gx <=> b.gx
WHERE a.cnt <> b.cnt;

-- 明细4: 证件号仅在库A
SELECT
    a.zjh,
    a.cnt AS cnt_a,
    'ONLY_ZJH_IN_A' AS diff_type
FROM zjh_cnt_a a
LEFT JOIN zjh_cnt_b b ON a.zjh <=> b.zjh
WHERE b.zjh IS NULL;

-- 明细5: 证件号仅在库B
SELECT
    b.zjh,
    b.cnt AS cnt_b,
    'ONLY_ZJH_IN_B' AS diff_type
FROM zjh_cnt_b b
LEFT JOIN zjh_cnt_a a ON a.zjh <=> b.zjh
WHERE a.zjh IS NULL;

-- 明细6: 证件号两侧都有，但该 zjh 下总条数不一致
SELECT
    a.zjh,
    a.cnt AS cnt_a,
    b.cnt AS cnt_b,
    a.cnt - b.cnt AS cnt_delta,
    'ZJH_CNT_DIFF' AS diff_type
FROM zjh_cnt_a a
INNER JOIN zjh_cnt_b b ON a.zjh <=> b.zjh
WHERE a.cnt <> b.cnt;
