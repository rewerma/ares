package com.github.ares.connector.doris.febe;

import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.RowKind;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.ares.com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ares.common.exceptions.AresException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes a batch of rows to Doris through FE stream load, which redirects to BE. */
final class StreamLoadClient {
    private static final Logger log = LoggerFactory.getLogger(StreamLoadClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_TIME_FRACTION =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final DorisFeBeConfig config;
    private final String[] fieldNames;
    private final int subtaskId;
    private int batchSeq;

    StreamLoadClient(DorisFeBeConfig config, String[] fieldNames, int subtaskId) {
        this.config = config;
        this.fieldNames = fieldNames;
        this.subtaskId = subtaskId;
    }

    void load(List<AresRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        boolean includeOp = needsOp(rows);
        byte[] body = toJson(rows, includeOp);
        String columns = columnsHeader(includeOp);
        AresException last = null;
        int attempts = config.getMaxRetries() + 1;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            String label = nextLabel();
            try {
                send(body, columns, label);
                return;
            } catch (AresException e) {
                last = e;
                log.warn(
                        "Doris stream load attempt {}/{} failed: {}",
                        attempt,
                        attempts,
                        e.getMessage());
                if (attempt < attempts) {
                    sleep(attempt);
                }
            }
        }
        throw last;
    }

    static byte[] toJson(String[] fieldNames, List<AresRow> rows, boolean includeOp) {
        ArrayNode array = MAPPER.createArrayNode();
        for (AresRow row : rows) {
            ObjectNode node = array.addObject();
            for (int i = 0; i < fieldNames.length; i++) {
                putValue(node, fieldNames[i], row.getField(i));
            }
            if (includeOp) {
                node.put("__DORIS_DELETE_SIGN__", opValue(row));
            }
        }
        try {
            return MAPPER.writeValueAsBytes(array);
        } catch (Exception e) {
            throw new AresException("Failed to serialize Doris stream load JSON", e);
        }
    }

    private byte[] toJson(List<AresRow> rows, boolean includeOp) {
        return toJson(fieldNames, rows, includeOp);
    }

    private void send(byte[] body, String columns, String label) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(
                "Authorization", DorisHttp.basicAuth(config.getUsername(), config.getPassword()));
        headers.put("Expect", "100-continue");
        headers.put("format", "json");
        headers.put("strip_outer_array", "true");
        headers.put("ignore_json_size", "true");
        headers.put("label", label);
        headers.put("columns", columns);
        if (config.isPartialUpdate()) {
            headers.put("partial_columns", "true");
        }
        if (config.getScanQueryTimeoutSec() > 0) {
            headers.put("timeout", config.getScanQueryTimeoutSec() + "");
        }
        headers.putAll(config.getStreamLoadProps());
        AresException last = null;
        for (String feNode : config.getFeNodes()) {
            String url =
                    "http://"
                            + feNode
                            + "/api/"
                            + config.getDatabase()
                            + "/"
                            + config.getTable()
                            + "/_stream_load";
            try {
                DorisHttp.HttpResult result =
                        DorisHttp.put(url, headers, body, config.getHttpSocketTimeoutMs(), true);
                assertSuccess(result, label);
                log.info("Doris stream load label {} finished: {}", label, abbreviate(result.body));
                return;
            } catch (AresException e) {
                last = e;
                log.warn("Stream load via {} failed: {}", feNode, e.getMessage());
            } catch (Exception e) {
                last = new AresException("Stream load via " + feNode + " failed", e);
                log.warn("Stream load via {} failed: {}", feNode, e.getMessage());
            }
        }
        if (last != null) {
            throw last;
        }
        throw new AresException("Doris stream load failed without an FE response");
    }

    static void assertSuccess(DorisHttp.HttpResult result, String label) {
        if (result.status >= 400) {
            throw new AresException(
                    "Doris stream load HTTP "
                            + result.status
                            + " label "
                            + label
                            + ": "
                            + abbreviate(result.body));
        }
        try {
            String status = MAPPER.readTree(result.body).path("Status").asText("");
            if ("Success".equalsIgnoreCase(status)
                    || "Publish Timeout".equalsIgnoreCase(status)
                    || "Label Already Exists".equalsIgnoreCase(status)) {
                return;
            }
            throw new AresException(
                    "Doris stream load label "
                            + label
                            + " status "
                            + status
                            + ": "
                            + abbreviate(result.body));
        } catch (AresException e) {
            throw e;
        } catch (Exception e) {
            throw new AresException(
                    "Doris stream load label "
                            + label
                            + " returned an unreadable response: "
                            + abbreviate(result.body),
                    e);
        }
    }

    private String columnsHeader(boolean includeOp) {
        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) {
                columns.append(",");
            }
            columns.append(fieldNames[i]);
        }
        if (includeOp) {
            if (columns.length() > 0) {
                columns.append(",");
            }
            columns.append("__DORIS_DELETE_SIGN__");
        }
        return columns.toString();
    }

    private String nextLabel() {
        batchSeq++;
        String label =
                "ares_"
                        + subtaskId
                        + "_"
                        + batchSeq
                        + "_"
                        + UUID.randomUUID().toString().replace("-", "");
        return label.length() <= 128 ? label : label.substring(0, 128);
    }

    private static boolean needsOp(List<AresRow> rows) {
        for (AresRow row : rows) {
            RowKind kind = row.getRowKind();
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                return true;
            }
        }
        return false;
    }

    private static int opValue(AresRow row) {
        RowKind kind = row.getRowKind();
        return kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE ? 1 : 0;
    }

    private static void putValue(ObjectNode node, String name, Object value) {
        if (value == null) {
            node.putNull(name);
        } else if (value instanceof Boolean) {
            node.put(name, (Boolean) value);
        } else if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            node.put(name, ((Number) value).intValue());
        } else if (value instanceof Long) {
            node.put(name, (Long) value);
        } else if (value instanceof Float) {
            node.put(name, (Float) value);
        } else if (value instanceof Double) {
            node.put(name, (Double) value);
        } else if (value instanceof BigDecimal) {
            node.put(name, (BigDecimal) value);
        } else if (value instanceof LocalDate) {
            node.put(name, DATE.format((LocalDate) value));
        } else if (value instanceof LocalTime) {
            node.put(name, TIME.format((LocalTime) value));
        } else if (value instanceof LocalDateTime) {
            LocalDateTime time = (LocalDateTime) value;
            node.put(
                    name,
                    time.getNano() == 0 ? DATE_TIME.format(time) : DATE_TIME_FRACTION.format(time));
        } else if (value instanceof byte[]) {
            node.put(name, Base64.getEncoder().encodeToString((byte[]) value));
        } else {
            node.put(name, String.valueOf(value));
        }
    }

    private static void sleep(int attempt) {
        try {
            Thread.sleep(1000L * attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AresException("Doris stream load retry was interrupted", e);
        }
    }

    private static String abbreviate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 500 ? body : body.substring(0, 500);
    }
}
