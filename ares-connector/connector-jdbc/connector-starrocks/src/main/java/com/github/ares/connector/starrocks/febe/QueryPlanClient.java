package com.github.ares.connector.starrocks.febe;

import com.github.ares.com.fasterxml.jackson.databind.JsonNode;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.common.exceptions.AresException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Asks FE for a tablet routing plan, then groups tablets into BE scan splits. */
public final class QueryPlanClient {
    private static final Logger log = LoggerFactory.getLogger(QueryPlanClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private QueryPlanClient() {}

    public static List<StarRocksSplit> planSplits(StarRocksFeBeConfig config) {
        String sql = scanSql(config);
        log.info("StarRocks febe query plan SQL: {}", sql);
        QueryPlan plan = requestPlan(config, sql);
        return toSplits(
                plan, config.getRequestTabletSize(), config.getDatabase(), config.getTable());
    }

    public static String scanSql(StarRocksFeBeConfig config) {
        if (config.getQuery() != null && !config.getQuery().trim().isEmpty()) {
            return config.getQuery().trim();
        }
        StringBuilder sql = new StringBuilder("SELECT ");
        List<String> columns = config.getColumns();
        if (columns == null || columns.isEmpty()) {
            sql.append("*");
        } else {
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(quote(columns.get(i)));
            }
        }
        sql.append(" FROM ")
                .append(quote(config.getDatabase()))
                .append(".")
                .append(quote(config.getTable()));
        if (config.getScanFilter() != null && !config.getScanFilter().isEmpty()) {
            sql.append(" WHERE ").append(config.getScanFilter());
        }
        return sql.toString();
    }

    static QueryPlan requestPlan(StarRocksFeBeConfig config, String sql) {
        List<String> nodes = new ArrayList<>(config.getFeNodes());
        Collections.shuffle(nodes);
        AresException last = null;
        for (String feNode : nodes) {
            String url =
                    "http://"
                            + feNode
                            + "/api/"
                            + config.getDatabase()
                            + "/"
                            + config.getTable()
                            + "/_query_plan";
            try {
                String body =
                        StarRocksHttp.postJson(
                                url,
                                config.getUsername(),
                                config.getPassword(),
                                "{\"sql\":" + MAPPER.writeValueAsString(sql) + "}",
                                config.getHttpSocketTimeoutMs());
                return parse(body);
            } catch (Exception e) {
                last =
                        e instanceof AresException
                                ? (AresException) e
                                : new AresException(
                                        "Failed to request StarRocks query plan from " + feNode, e);
                log.warn("Request query plan from {} failed: {}", feNode, e.getMessage());
            }
        }
        if (last != null) {
            throw last;
        }
        throw new AresException("StarRocks query plan failed with an empty response");
    }

    public static QueryPlan parse(String body) {
        try {
            JsonNode root = MAPPER.readTree(body);
            JsonNode status = root.get("status");
            if (status != null && status.isNumber() && status.asInt() != 200) {
                throw new AresException("StarRocks query plan failed: " + abbreviate(body));
            }
            JsonNode planNode = root.get("opaqued_query_plan");
            if (planNode == null || planNode.asText().isEmpty()) {
                throw new AresException(
                        "StarRocks query plan has no opaqued_query_plan: " + abbreviate(body));
            }
            JsonNode partitions = root.get("partitions");
            Map<Long, List<String>> tablets = new LinkedHashMap<>();
            if (partitions != null && partitions.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = partitions.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    List<String> routings = new ArrayList<>();
                    JsonNode routingNode = entry.getValue().get("routings");
                    if (routingNode != null && routingNode.isArray()) {
                        for (JsonNode routing : routingNode) {
                            routings.add(routing.asText());
                        }
                    }
                    tablets.put(Long.valueOf(entry.getKey()), routings);
                }
            }
            if (tablets.isEmpty()) {
                throw new AresException("StarRocks query plan has no tablets: " + abbreviate(body));
            }
            return new QueryPlan(planNode.asText(), tablets);
        } catch (AresException e) {
            throw e;
        } catch (Exception e) {
            throw new AresException("Failed to parse StarRocks query plan: " + abbreviate(body), e);
        }
    }

    public static List<StarRocksSplit> toSplits(
            QueryPlan plan, int tabletSize, String database, String table) {
        Map<String, List<Long>> beToTablets = assignTablets(plan.getTablets());
        int groupSize = tabletSize <= 0 ? Integer.MAX_VALUE : tabletSize;
        List<StarRocksSplit> splits = new ArrayList<>();
        int splitIndex = 0;
        for (Map.Entry<String, List<Long>> entry : beToTablets.entrySet()) {
            List<Long> tablets = entry.getValue();
            for (int offset = 0; offset < tablets.size(); offset += groupSize) {
                int end = (int) Math.min(tablets.size(), (long) offset + groupSize);
                List<Long> group = new ArrayList<>(tablets.subList(offset, end));
                splits.add(
                        new StarRocksSplit(
                                entry.getKey() + "-" + splitIndex,
                                entry.getKey(),
                                group,
                                plan.getOpaqueQueryPlan(),
                                database,
                                table));
                splitIndex++;
            }
        }
        return splits;
    }

    static Map<String, List<Long>> assignTablets(Map<Long, List<String>> tablets) {
        Map<String, List<Long>> beToTablets = new LinkedHashMap<>();
        for (Map.Entry<Long, List<String>> tablet : tablets.entrySet()) {
            String chosen = null;
            int least = Integer.MAX_VALUE;
            for (String be : tablet.getValue()) {
                List<Long> assigned = beToTablets.get(be);
                int size = assigned == null ? 0 : assigned.size();
                if (chosen == null || size < least) {
                    chosen = be;
                    least = size;
                }
            }
            if (chosen == null) {
                throw new AresException("Tablet " + tablet.getKey() + " has no BE routing");
            }
            beToTablets.computeIfAbsent(chosen, key -> new ArrayList<>()).add(tablet.getKey());
        }
        return beToTablets;
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String abbreviate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 500 ? body : body.substring(0, 500);
    }

    public static final class QueryPlan {
        private final String opaqueQueryPlan;
        private final Map<Long, List<String>> tablets;

        public QueryPlan(String opaqueQueryPlan, Map<Long, List<String>> tablets) {
            this.opaqueQueryPlan = opaqueQueryPlan;
            this.tablets = tablets;
        }

        public String getOpaqueQueryPlan() {
            return opaqueQueryPlan;
        }

        public Map<Long, List<String>> getTablets() {
            return tablets;
        }
    }
}
