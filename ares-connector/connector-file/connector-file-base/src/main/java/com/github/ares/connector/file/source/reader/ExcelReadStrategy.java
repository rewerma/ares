package com.github.ares.connector.file.source.reader;

import com.github.ares.api.source.Collector;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.TimeUtils;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.exception.FileConnectorException;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import com.github.ares.common.exceptions.CommonErrorCode;

public class ExcelReadStrategy extends AbstractReadStrategy {

    private final DateUtils.Formatter dateFormat = DateUtils.Formatter.YYYY_MM_DD;

    private final DateTimeUtils.Formatter datetimeFormat = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    private final TimeUtils.Formatter timeFormat = TimeUtils.Formatter.HH_MM_SS;

    private int[] indexes;

    private int cellCount;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public void read(String path, String tableId, Collector<AresRow> output) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        FSDataInputStream file = hadoopFileSystemProxy.getInputStream(path);
        Workbook workbook;
        if (path.endsWith(".xls")) {
            workbook = new HSSFWorkbook(file);
        } else if (path.endsWith(".xlsx")) {
            workbook = new XSSFWorkbook(file);
        } else {
            throw new FileConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    "Only support read excel file");
        }
        Sheet sheet =
                pluginConfig.hasPath(BaseSourceConfigOptions.SHEET_NAME.key())
                        ? workbook.getSheet(
                                pluginConfig.getString(BaseSourceConfigOptions.SHEET_NAME.key()))
                        : workbook.getSheetAt(0);
        cellCount = aresRowType.getTotalFields();
        cellCount = partitionsMap.isEmpty() ? cellCount : cellCount + partitionsMap.size();
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        int rowCount = sheet.getPhysicalNumberOfRows();
        if (skipHeaderNumber > Integer.MAX_VALUE
                || skipHeaderNumber < Integer.MIN_VALUE
                || skipHeaderNumber > rowCount) {
            throw new FileConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    "Skip the number of rows exceeds the maximum or minimum limit of Sheet");
        }
        IntStream.range((int) skipHeaderNumber, rowCount)
                .mapToObj(sheet::getRow)
                .filter(Objects::nonNull)
                .forEach(
                        rowData -> {
                            int[] cellIndexes =
                                    indexes == null
                                            ? IntStream.range(0, cellCount).toArray()
                                            : indexes;
                            int z = 0;
                            AresRow aresRow = new AresRow(cellCount);
                            for (int j : cellIndexes) {
                                Cell cell = rowData.getCell(j);
                                aresRow.setField(
                                        z++,
                                        cell == null
                                                ? null
                                                : convert(
                                                        getCellValue(cell.getCellType(), cell),
                                                        fieldTypes[z - 1]));
                            }
                            if (isMergePartition) {
                                int index = aresRowType.getTotalFields();
                                for (String value : partitionsMap.values()) {
                                    aresRow.setField(index++, value);
                                }
                            }
                            aresRow.setTableId(tableId);
                            output.collect(aresRow);
                        });
    }

    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        if (isNullOrEmpty(aresRowType.getFieldNames())
                || isNullOrEmpty(aresRowType.getFieldTypes())) {
            throw new FileConnectorException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    "Schmea information is not set or incorrect schmea settings");
        }
        AresRowType userDefinedRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), aresRowType);
        // column projection
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            // get the read column index from user-defined row type
            indexes = new int[readColumns.size()];
            String[] fields = new String[readColumns.size()];
            AresDataType<?>[] types = new AresDataType[readColumns.size()];
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = aresRowType.indexOf(readColumns.get(i));
                fields[i] = aresRowType.getFieldName(indexes[i]);
                types[i] = aresRowType.getFieldType(indexes[i]);
            }
            this.aresRowType = new AresRowType(fields, types);
            this.aresRowTypeWithPartition =
                    mergePartitionTypes(fileNames.get(0), this.aresRowType);
        } else {
            this.aresRowType = aresRowType;
            this.aresRowTypeWithPartition = userDefinedRowTypeWithPartition;
        }
    }

    @Override
    public AresRowType getAresRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }

    private Object getCellValue(CellType cellType, Cell cell) {
        switch (cellType) {
            case STRING:
                return cell.getStringCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    DataFormatter formatter = new DataFormatter();
                    return formatter.formatCellValue(cell);
                }
                return cell.getNumericCellValue();
            case ERROR:
                break;
            default:
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format("[%s] type not support ", cellType));
        }
        return null;
    }

    @SneakyThrows
    private Object convert(Object field, AresDataType<?> fieldType) {
        if (field == null) {
            return "";
        }
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case MAP:
            case ARRAY:
                return objectMapper.readValue((String) field, fieldType.getTypeClass());
            case STRING:
                return field;
            case DOUBLE:
                return Double.parseDouble(field.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(field.toString());
            case FLOAT:
                return (float) Double.parseDouble(field.toString());
            case BIGINT:
                return (long) Double.parseDouble(field.toString());
            case INT:
                return (int) Double.parseDouble(field.toString());
            case TINYINT:
                return (byte) Double.parseDouble(field.toString());
            case SMALLINT:
                return (short) Double.parseDouble(field.toString());
            case DECIMAL:
                return BigDecimal.valueOf(Double.parseDouble(field.toString()));
            case DATE:
                return LocalDate.parse(
                        (String) field, DateTimeFormatter.ofPattern(dateFormat.getValue()));
            case TIME:
                return LocalTime.parse(
                        (String) field, DateTimeFormatter.ofPattern(timeFormat.getValue()));
            case TIMESTAMP:
                return LocalDateTime.parse(
                        (String) field, DateTimeFormatter.ofPattern(datetimeFormat.getValue()));
            case NULL:
                return "";
            case BYTES:
                return field.toString().getBytes(StandardCharsets.UTF_8);
            case ROW:
                String delimiter =
                        ReadonlyConfig.fromConfig(pluginConfig)
                                .get(BaseSourceConfigOptions.FIELD_DELIMITER);
                String[] context = field.toString().split(delimiter);
                AresRowType ft = (AresRowType) fieldType;
                int length = context.length;
                AresRow aresRow = new AresRow(length);
                for (int j = 0; j < length; j++) {
                    aresRow.setField(j, convert(context[j], ft.getFieldType(j)));
                }
                return aresRow;
            default:
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "User defined schema validation failed");
        }
    }

    private <T> boolean isNullOrEmpty(T[] arr) {
        return arr == null || arr.length == 0;
    }
}
