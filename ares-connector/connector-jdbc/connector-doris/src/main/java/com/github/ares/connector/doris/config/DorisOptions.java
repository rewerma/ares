package com.github.ares.connector.doris.config;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import java.util.Map;

public final class DorisOptions {
    public static final String JDBC = "jdbc";
    public static final String FEBE = "febe";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final Option<String> MODE =
            Options.key("mode")
                    .stringType()
                    .defaultValue(JDBC)
                    .withDescription("Doris access mode: jdbc or febe");

    public static final Option<String> FE_NODES =
            Options.key("fe_nodes")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("node_urls", "nodeUrls")
                    .withDescription(
                            "FE HTTP addresses used by febe mode, for example 127.0.0.1:8030. "
                                    + "Multiple addresses are separated by commas.");

    public static final Option<Integer> REQUEST_TABLET_SIZE =
            Options.key("request_tablet_size")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("Maximum number of tablets read by one BE scan split");

    public static final Option<String> SCAN_FILTER =
            Options.key("scan_filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Filter appended to the FE query plan SQL, without WHERE");

    public static final Option<Integer> SCAN_CONNECT_TIMEOUT_MS =
            Options.key("scan_connect_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Timeout for opening a BE scan socket");

    public static final Option<Integer> SCAN_BATCH_ROWS =
            Options.key("scan_batch_rows")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Batch size requested from each BE scanner");

    public static final Option<Integer> SCAN_KEEP_ALIVE_MIN =
            Options.key("scan_keep_alive_min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("BE scanner keep-alive minutes");

    public static final Option<Long> SCAN_MEM_LIMIT =
            Options.key("scan_mem_limit")
                    .longType()
                    .defaultValue(1024L * 1024L * 1024L)
                    .withDescription("Memory byte limit of one BE scan");

    public static final Option<Long> BATCH_MAX_BYTES =
            Options.key("batch_max_bytes")
                    .longType()
                    .defaultValue(10L * 1024L * 1024L)
                    .withDescription(
                            "Flush a stream load when the buffered JSON reaches this size");

    public static final Option<Integer> HTTP_SOCKET_TIMEOUT_MS =
            Options.key("http_socket_timeout_ms")
                    .intType()
                    .defaultValue(180000)
                    .withDescription("HTTP timeout for FE query plan and stream load");

    public static final Option<Map<String, String>> STREAM_LOAD_PROPS =
            Options.key("stream_load_props")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Extra HTTP headers passed to Doris stream load");

    public static final Option<Boolean> PARTIAL_UPDATE =
            Options.key("partial_update")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable stream load partial column update. The target table must use the unique key model. Sent as the partial_columns header.");

    private DorisOptions() {}
}
