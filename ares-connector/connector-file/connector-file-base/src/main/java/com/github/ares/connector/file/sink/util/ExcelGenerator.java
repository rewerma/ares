package com.github.ares.connector.file.sink.util;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.common.utils.DateTimeUtils;
import com.github.ares.common.utils.DateUtils;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.common.utils.TimeUtils;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class ExcelGenerator {
    private final List<Integer> sinkColumnsIndexInRow;
    private final AresRowType aresRowType;
    private final DateUtils.Formatter dateFormat;
    private final DateTimeUtils.Formatter dateTimeFormat;
    private final TimeUtils.Formatter timeFormat;
    private final String fieldDelimiter;
    private final Workbook wb;
    private final CellStyle wholeNumberCellStyle;
    private final CellStyle stringCellStyle;
    private final CellStyle dateCellStyle;
    private final CellStyle dateTimeCellStyle;
    private final CellStyle timeCellStyle;
    private final Sheet st;
    private int row = 0;

    public ExcelGenerator(
            List<Integer> sinkColumnsIndexInRow,
            AresRowType aresRowType,
            FileSinkConfig fileSinkConfig) {
        this.sinkColumnsIndexInRow = sinkColumnsIndexInRow;
        this.aresRowType = aresRowType;
        if (fileSinkConfig.getMaxRowsInMemory() > 0) {
            wb = new SXSSFWorkbook(fileSinkConfig.getMaxRowsInMemory());
        } else {
            wb = new SXSSFWorkbook();
        }
        Optional<String> sheetName = Optional.ofNullable(fileSinkConfig.getSheetName());
        Random random = new Random();
        this.st =
                wb.createSheet(
                        sheetName.orElseGet(() -> String.format("Sheet%d", random.nextInt())));
        Row row = st.createRow(this.row);
        for (Integer i : sinkColumnsIndexInRow) {
            String fieldName = aresRowType.getFieldName(i);
            row.createCell(i).setCellValue(fieldName);
        }
        this.dateFormat = fileSinkConfig.getDateFormat();
        this.dateTimeFormat = fileSinkConfig.getDatetimeFormat();
        this.timeFormat = fileSinkConfig.getTimeFormat();
        this.fieldDelimiter = fileSinkConfig.getFieldDelimiter();
        wholeNumberCellStyle = createStyle(wb, "General");
        stringCellStyle = createStyle(wb, "@");
        dateCellStyle = createStyle(wb, dateFormat.getValue());
        dateTimeCellStyle = createStyle(wb, dateTimeFormat.getValue());
        timeCellStyle = createStyle(wb, timeFormat.getValue());

        this.row += 1;
    }

    public void writeData(AresRow aresRow) {
        Row excelRow = this.st.createRow(this.row);
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        for (Integer i : sinkColumnsIndexInRow) {
            Cell cell = excelRow.createCell(i);
            Object value = aresRow.getField(i);
            setCellValue(fieldTypes[i], aresRowType.getFieldName(i), value, cell);
        }
        this.row += 1;
    }

    public void flushAndCloseExcel(OutputStream output) throws IOException {
        wb.write(output);
        wb.close();
    }

    private void setCellValue(
            AresDataType<?> type, String fieldName, Object value, Cell cell) {
        if (value == null) {
            cell.setBlank();
        } else {
            switch (type.getSqlType()) {
                case STRING:
                    cell.setCellValue((String) value);
                    cell.setCellStyle(stringCellStyle);
                    break;
                case BOOLEAN:
                    cell.setCellValue((Boolean) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case SMALLINT:
                    cell.setCellValue((short) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case TINYINT:
                    cell.setCellValue((byte) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case INT:
                    cell.setCellValue((int) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case BIGINT:
                    cell.setCellValue((long) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case FLOAT:
                    cell.setCellValue((float) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case DOUBLE:
                    cell.setCellValue((double) value);
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case DECIMAL:
                    cell.setCellValue(Double.parseDouble(value.toString()));
                    cell.setCellStyle(wholeNumberCellStyle);
                    break;
                case BYTES:
                    List<String> arrayData = new ArrayList<>();
                    for (int i = 0; i < Array.getLength(value); i++) {
                        arrayData.add(String.valueOf(Array.get(value, i)));
                    }
                    cell.setCellValue(arrayData.toString());
                    cell.setCellStyle(stringCellStyle);
                    break;
                case MAP:
                case ARRAY:
                    cell.setCellValue(JsonUtils.toJsonString(value));
                    cell.setCellStyle(stringCellStyle);
                    break;
                case ROW:
                    Object[] fields = ((AresRow) value).getFields();
                    String[] strings = new String[fields.length];
                    for (int i = 0; i < fields.length; i++) {
                        strings[i] =
                                convert(
                                        ((AresRowType) type).getFieldName(i),
                                        fields[i],
                                        ((AresRowType) type).getFieldType(i));
                    }
                    cell.setCellValue(String.join(fieldDelimiter, strings));
                    cell.setCellStyle(stringCellStyle);
                    break;
                case DATE:
                    cell.setCellValue((LocalDate) value);
                    cell.setCellStyle(dateCellStyle);
                    break;
                case TIMESTAMP:
                case TIME:
                    setTimestampColumn(value, cell);
                    break;
                default:
                    throw CommonError.unsupportedDataType(
                            "Excel", type.getSqlType().toString(), fieldName);
            }
        }
    }

    private String convert(String fieldName, Object field, AresDataType<?> fieldType) {
        if (field == null) {
            return "";
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
            case MAP:
                return JsonUtils.toJsonString(field);
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return field.toString();
            case DATE:
                return DateUtils.toString((LocalDate) field, dateFormat);
            case TIME:
                return TimeUtils.toString((LocalTime) field, timeFormat);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) field, dateTimeFormat);
            case NULL:
                return "";
            case BYTES:
                return new String((byte[]) field);
            case ROW:
                Object[] fields = ((AresRow) field).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] =
                            convert(
                                    ((AresRowType) fieldType).getFieldName(i),
                                    fields[i],
                                    ((AresRowType) fieldType).getFieldType(i));
                }
                return String.join(fieldDelimiter, strings);
            default:
                throw CommonError.unsupportedDataType(
                        "Excel", fieldType.getSqlType().toString(), fieldName);
        }
    }

    private void setTimestampColumn(Object value, Cell cell) {
        if (value instanceof Timestamp) {
            cell.setCellValue((Timestamp) value);
            cell.setCellStyle(dateTimeCellStyle);
        } else if (value instanceof LocalDate) {
            cell.setCellValue((LocalDate) value);
            cell.setCellStyle(dateCellStyle);
        } else if (value instanceof LocalDateTime) {
            cell.setCellValue(Timestamp.valueOf((LocalDateTime) value));
            cell.setCellStyle(dateTimeCellStyle);
        } else if (value instanceof LocalTime) {
            cell.setCellValue(
                    Timestamp.valueOf(((LocalTime) value).atDate(LocalDate.ofEpochDay(0))));
            cell.setCellStyle(timeCellStyle);
        } else {
            throw new FileConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Time series type expected for field");
        }
    }

    private CellStyle createStyle(Workbook wb, String format) {
        CreationHelper creationHelper = wb.getCreationHelper();
        CellStyle cellStyle = wb.createCellStyle();
        cellStyle.setDataFormat(creationHelper.createDataFormat().getFormat(format));
        return cellStyle;
    }
}
