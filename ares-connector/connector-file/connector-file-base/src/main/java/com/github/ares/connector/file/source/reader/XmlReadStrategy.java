package com.github.ares.connector.file.source.reader;

import com.github.ares.api.source.Collector;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.TimeUtils;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** The XmlReadStrategy class is used to read data from XML files in Ares. */
@Slf4j
public class XmlReadStrategy extends AbstractReadStrategy {

    private String tableRowName;
    private Boolean useAttrFormat;
    private String delimiter;

    private int fieldCount;

    private DateUtils.Formatter dateFormat;
    private DateTimeUtils.Formatter datetimeFormat;
    private TimeUtils.Formatter timeFormat;
    private String encoding = BaseSourceConfigOptions.ENCODING.defaultValue();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        preCheckAndInitializeConfiguration();
    }

    @Override
    public void read(String path, String tableId, Collector<AresRow> output)
            throws IOException, FileConnectorException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        SAXReader saxReader = new SAXReader();
        Document document;
        try {
            document =
                    saxReader.read(
                            new InputStreamReader(
                                    hadoopFileSystemProxy.getInputStream(path), encoding));
        } catch (DocumentException e) {
            throw new FileConnectorException(
                    FileConnectorErrorCode.FILE_READ_FAILED, "Failed to read xml file: " + path, e);
        }
        Element rootElement = document.getRootElement();

        fieldCount =
                isMergePartition
                        ? aresRowTypeWithPartition.getTotalFields()
                        : aresRowType.getTotalFields();

        rootElement
                .selectNodes(getXPathExpression(tableRowName))
                .forEach(
                        node -> {
                            AresRow aresRow = new AresRow(fieldCount);

                            List<? extends Node> fields =
                                    new ArrayList<>(
                                                    (useAttrFormat
                                                            ? ((Element) node).attributes()
                                                            : node.selectNodes("./*")))
                                            .stream()
                                                    .filter(
                                                            field ->
                                                                    ArrayUtils.contains(
                                                                            aresRowType
                                                                                    .getFieldNames(),
                                                                            field.getName()))
                                                    .collect(Collectors.toList());

                            if (CollectionUtils.isEmpty(fields)) return;

                            fields.forEach(
                                    field -> {
                                        int fieldIndex =
                                                ArrayUtils.indexOf(
                                                        aresRowType.getFieldNames(),
                                                        field.getName());
                                        aresRow.setField(
                                                fieldIndex,
                                                convert(
                                                        field.getText(),
                                                        aresRowType
                                                                .getFieldTypes()[fieldIndex]));
                                    });

                            if (isMergePartition) {
                                int partitionIndex = aresRowType.getTotalFields();
                                for (String value : partitionsMap.values()) {
                                    aresRow.setField(partitionIndex++, value);
                                }
                            }

                            aresRow.setTableId(tableId);
                            output.collect(aresRow);
                        });
    }

    @Override
    public AresRowType getAresRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                "User must defined schema for xml file type");
    }

    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        if (ArrayUtils.isEmpty(aresRowType.getFieldNames())
                || ArrayUtils.isEmpty(aresRowType.getFieldTypes())) {
            throw new FileConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Schema information is undefined or misconfigured, please check your configuration file.");
        }

        if (readColumns.isEmpty()) {
            this.aresRowType = aresRowType;
            this.aresRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), aresRowType);
        } else {
            if (readColumns.retainAll(Arrays.asList(aresRowType.getFieldNames()))) {
                log.warn(
                        "The read columns configuration will be filtered by the schema configuration, this may cause the actual results to be inconsistent with expectations. This is due to read columns not being a subset of the schema, "
                                + "maybe you should check the schema and read_columns!");
            }
            int[] indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            AresDataType<?>[] types = new AresDataType[readColumns.size()];
            for (int i = 0; i < readColumns.size(); i++) {
                indexes[i] = aresRowType.indexOf(readColumns.get(i));
                fields[i] = aresRowType.getFieldName(indexes[i]);
                types[i] = aresRowType.getFieldType(indexes[i]);
            }
            this.aresRowType = new AresRowType(fields, types);
            this.aresRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.aresRowType);
        }
    }

    @SneakyThrows
    private Object convert(String fieldValue, AresDataType<?> fieldType) {
        if (StringUtils.isBlank(fieldValue)) {
            return "";
        }
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case STRING:
                return fieldValue;
            case DATE:
                return DateUtils.parse(fieldValue, dateFormat);
            case TIME:
                return TimeUtils.parse(fieldValue, timeFormat);
            case TIMESTAMP:
                return DateTimeUtils.parse(fieldValue, datetimeFormat);
            case TINYINT:
                return (byte) Double.parseDouble(fieldValue);
            case SMALLINT:
                return (short) Double.parseDouble(fieldValue);
            case INT:
                return (int) Double.parseDouble(fieldValue);
            case BIGINT:
                return new BigDecimal(fieldValue).longValue();
            case DOUBLE:
                return Double.parseDouble(fieldValue);
            case FLOAT:
                return (float) Double.parseDouble(fieldValue);
            case DECIMAL:
                return new BigDecimal(fieldValue);
            case BOOLEAN:
                return Boolean.parseBoolean(fieldValue);
            case BYTES:
                return fieldValue.getBytes(StandardCharsets.UTF_8);
            case NULL:
                return "";
            case ROW:
                String[] context = fieldValue.split(delimiter);
                AresRowType ft = (AresRowType) fieldType;
                AresRow row = new AresRow(context.length);
                IntStream.range(0, context.length)
                        .forEach(i -> row.setField(i, convert(context[i], ft.getFieldTypes()[i])));
                return row;
            case MAP:
            case ARRAY:
                return objectMapper.readValue(fieldValue, fieldType.getTypeClass());
            default:
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("Unsupported data type: %s", sqlType));
        }
    }

    private String getXPathExpression(String tableRowIdentification) {
        return String.format("//%s", tableRowIdentification);
    }

    /** Performs pre-checks and initialization of the configuration for reading XML files. */
    private void preCheckAndInitializeConfiguration() {
        this.tableRowName = getPrimitiveConfigValue(BaseSourceConfigOptions.XML_ROW_TAG);
        this.useAttrFormat = getPrimitiveConfigValue(BaseSourceConfigOptions.XML_USE_ATTR_FORMAT);

        // Check mandatory configurations
        if (StringUtils.isEmpty(tableRowName) || useAttrFormat == null) {
            throw new FileConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Mandatory configurations '%s' and '%s' must be specified when reading XML files.",
                            BaseSourceConfigOptions.XML_ROW_TAG.key(),
                            BaseSourceConfigOptions.XML_USE_ATTR_FORMAT.key()));
        }

        this.delimiter = getPrimitiveConfigValue(BaseSourceConfigOptions.FIELD_DELIMITER);

        this.dateFormat =
                getComplexDateConfigValue(
                        BaseSourceConfigOptions.DATE_FORMAT, DateUtils.Formatter::parse);
        this.timeFormat =
                getComplexDateConfigValue(
                        BaseSourceConfigOptions.TIME_FORMAT, TimeUtils.Formatter::parse);
        this.datetimeFormat =
                getComplexDateConfigValue(
                        BaseSourceConfigOptions.DATETIME_FORMAT, DateTimeUtils.Formatter::parse);
        this.encoding =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
    }

    /**
     * Retrieves the value of a primitive configuration option.
     *
     * @param option the configuration option to retrieve the value for
     * @param <T> the type of the configuration option
     * @return the value of the configuration option, or the default value if the option is not set
     */
    @SuppressWarnings("unchecked")
    private <T> T getPrimitiveConfigValue(Option<?> option) {
        if (!pluginConfig.hasPath(option.key())) {
            return (T) option.defaultValue();
        }
        return (T) pluginConfig.getAnyRef(option.key());
    }

    /**
     * Retrieves the complex date configuration value for the given option.
     *
     * @param option The configuration option to retrieve.
     * @param parser The function used to parse the configuration value.
     * @param <T> The type of the configuration value.
     * @return The parsed configuration value or the default value if not found.
     */
    @SuppressWarnings("unchecked")
    private <T> T getComplexDateConfigValue(Option<?> option, Function<String, T> parser) {
        if (!pluginConfig.hasPath(option.key())) {
            return (T) option.defaultValue();
        }
        return parser.apply(pluginConfig.getString(option.key()));
    }
}
