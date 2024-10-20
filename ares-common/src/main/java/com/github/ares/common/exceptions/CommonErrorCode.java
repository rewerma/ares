package com.github.ares.common.exceptions;

/** Ares connector error code interface */
public enum CommonErrorCode implements AresErrorCode {
    ILLEGAL_ARGUMENT("COMMON-06", "Illegal argument"),
    FILE_OPERATION_FAILED("COMMON-01", "<identifier> <operation> file '<fileName>' failed."),
    JSON_OPERATION_FAILED(
            "COMMON-02", "<identifier> JSON convert/parse '<payload>' operation failed."),
    UNSUPPORTED_OPERATION("COMMON-05", "Unsupported operation"),
    UNSUPPORTED_DATA_TYPE(
            "COMMON-07", "'<identifier>' unsupported data type '<dataType>' of '<field>'"),
    UNSUPPORTED_ENCODING("COMMON-08", "unsupported encoding '<encoding>'"),
    TABLE_SCHEMA_GET_FAILED("COMMON-09", "Get table schema from upstream data failed"),
    FLUSH_DATA_FAILED("COMMON-10", "Flush data operation that in sink connector failed"),
    WRITER_OPERATION_FAILED(
            "COMMON-11", "Sink writer operation failed, such as (open, close) etc..."),
    READER_OPERATION_FAILED(
            "COMMON-12", "Source reader operation failed, such as (open, close) etc..."),
    CONVERT_TO_ARES_TYPE_ERROR(
            "COMMON-16",
            "'<connector>' <type> unsupported convert type '<dataType>' of '<field>' to Ares data type."),
    CONVERT_TO_ARES_TYPE_ERROR_SIMPLE(
            "COMMON-17",
            "'<identifier>' unsupported convert type '<dataType>' of '<field>' to Ares data type."),
    CONVERT_TO_CONNECTOR_TYPE_ERROR(
            "COMMON-18",
            "'<connector>' <type> unsupported convert Ares data type '<dataType>' of '<field>' to connector data type."),
    CONVERT_TO_CONNECTOR_TYPE_ERROR_SIMPLE(
            "COMMON-19",
            "'<identifier>' unsupported convert Ares data type '<dataType>' of '<field>' to connector data type."),
    GET_CATALOG_TABLE_WITH_UNSUPPORTED_TYPE_ERROR(
            "COMMON-20",
            "'<catalogName>' table '<tableName>' unsupported get catalog table with field data types '<fieldWithDataTypes>'"),
    GET_CATALOG_TABLES_WITH_UNSUPPORTED_TYPE_ERROR(
            "COMMON-21",
            "'<catalogName>' tables unsupported get catalog table，the corresponding field types in the following tables are not supported: '<tableUnsupportedTypes>'"),
    FILE_NOT_EXISTED(
            "COMMON-22",
            "<identifier> <operation> file '<fileName>' failed, because it not existed."),
    WRITE_ARES_ROW_ERROR(
            "COMMON-23",
            "<connector> write AresRow failed, the AresRow value is '<aresRow>'.");

    private final String code;
    private final String description;

    CommonErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
