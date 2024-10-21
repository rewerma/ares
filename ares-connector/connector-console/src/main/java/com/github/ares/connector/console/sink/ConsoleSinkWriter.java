package com.github.ares.connector.console.sink;

import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.sink.SupportMultiTableSinkWriter;
import com.github.ares.api.table.event.SchemaChangeEvent;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.connector.sink.AbstractSinkWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ConsoleSinkWriter extends AbstractSinkWriter<AresRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private AresRowType aresRowType;
    private final AtomicLong rowCounter = new AtomicLong(0);
    private final SinkWriter.Context context;

    boolean isPrintData = true;
    int delayMs = 0;

    public ConsoleSinkWriter(
            AresRowType aresRowType,
            SinkWriter.Context context,
            boolean isPrintData,
            int delayMs) {
        this.aresRowType = aresRowType;
        this.context = context;
        this.isPrintData = isPrintData;
        this.delayMs = delayMs;
        log.info("output rowType: {}", fieldsInfo(aresRowType));
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        log.info("changed rowType before: {}", fieldsInfo(aresRowType));
        log.info("changed rowType after: {}", fieldsInfo(aresRowType));
    }

    @Override
    public void write(AresRow element) {
        String[] arr = new String[aresRowType.getTotalFields()];
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        Object[] fields = element.getFields();
        for (int i = 0; i < fieldTypes.length; i++) {
            arr[i] = fieldToString(fieldTypes[i], fields[i]);
        }
        if (isPrintData) {
            log.info(
                    "subtaskIndex={}  rowIndex={}:  {}",
                    context.getIndexOfSubtask(),
                    rowCounter.incrementAndGet(),
                    StringUtils.join(arr, ", "));
        }
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AresException(e);
            }
        }
    }

    @Override
    public void close() {
        // nothing
    }

    private String fieldsInfo(AresRowType aresRowType) {
        String[] fieldsInfo = new String[aresRowType.getTotalFields()];
        for (int i = 0; i < aresRowType.getTotalFields(); i++) {
            fieldsInfo[i] =
                    String.format(
                            "%s<%s>",
                            aresRowType.getFieldName(i), aresRowType.getFieldType(i));
        }
        return StringUtils.join(fieldsInfo, ", ");
    }

    private String fieldToString(AresDataType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case ARRAY:
            case BYTES:
                List<String> arrayData = new ArrayList<>();
                for (int i = 0; i < Array.getLength(value); i++) {
                    arrayData.add(String.valueOf(Array.get(value, i)));
                }
                return arrayData.toString();
            case MAP:
                return JsonUtils.toJsonString(value);
            case ROW:
                List<String> rowData = new ArrayList<>();
                AresRowType rowType = (AresRowType) type;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    rowData.add(
                            fieldToString(
                                    rowType.getFieldTypes()[i],
                                    ((AresRow) value).getField(i)));
                }
                return rowData.toString();
            default:
                return String.valueOf(value);
        }
    }
}
