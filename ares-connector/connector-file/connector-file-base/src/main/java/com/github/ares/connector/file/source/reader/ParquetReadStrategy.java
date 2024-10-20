package com.github.ares.connector.file.source.reader;

import com.github.ares.api.source.Collector;
import com.github.ares.api.table.catalog.PrimitiveByteArrayType;
import com.github.ares.api.table.catalog.TablePath;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.exception.FileConnectorErrorCode;
import com.github.ares.connector.file.exception.FileConnectorException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
public class ParquetReadStrategy extends AbstractReadStrategy {
    private static final byte[] PARQUET_MAGIC =
            new byte[] {(byte) 'P', (byte) 'A', (byte) 'R', (byte) '1'};
    private static final long NANOS_PER_MILLISECOND = 1000000;
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);
    private static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;
    private static final String PARQUET = "Parquet";

    private int[] indexes;

    @Override
    public void read(String path, String tableId, Collector<AresRow> output)
            throws FileConnectorException, IOException {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            String errorMsg =
                    String.format(
                            "This file [%s] is not a parquet file, please check the format of this file",
                            path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
        Path filePath = new Path(path);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        HadoopInputFile hadoopInputFile =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        (configuration, userGroupInformation) ->
                                HadoopInputFile.fromPath(filePath, configuration));
        int fieldsCount = aresRowType.getTotalFields();
        GenericData dataModel = new GenericData();
        dataModel.addLogicalTypeConversion(new Conversions.DecimalConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        GenericRecord record;
        try (ParquetReader<GenericData.Record> reader =
                AvroParquetReader.<GenericData.Record>builder(hadoopInputFile)
                        .withDataModel(dataModel)
                        .build()) {
            while ((record = reader.read()) != null) {
                Object[] fields;
                if (isMergePartition) {
                    int index = fieldsCount;
                    fields = new Object[fieldsCount + partitionsMap.size()];
                    for (String value : partitionsMap.values()) {
                        fields[index++] = value;
                    }
                } else {
                    fields = new Object[fieldsCount];
                }
                for (int i = 0; i < fieldsCount; i++) {
                    Object data = record.get(indexes[i]);
                    fields[i] = resolveObject(data, aresRowType.getFieldType(i));
                }
                AresRow aresRow = new AresRow(fields);
                aresRow.setTableId(tableId);
                output.collect(aresRow);
            }
        }
    }

    private Object resolveObject(Object field, AresDataType<?> fieldType) {
        if (field == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayList<Object> origArray = new ArrayList<>();
                ((GenericData.Array<?>) field)
                        .iterator()
                        .forEachRemaining(
                                ele -> {
                                    if (ele instanceof Utf8) {
                                        origArray.add(ele.toString());
                                    } else {
                                        origArray.add(ele);
                                    }
                                });
                AresDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                switch (elementType.getSqlType()) {
                    case STRING:
                        return origArray.toArray(TYPE_ARRAY_STRING);
                    case BOOLEAN:
                        return origArray.toArray(TYPE_ARRAY_BOOLEAN);
                    case TINYINT:
                        return origArray.toArray(TYPE_ARRAY_BYTE);
                    case SMALLINT:
                        return origArray.toArray(TYPE_ARRAY_SHORT);
                    case INT:
                        return origArray.toArray(TYPE_ARRAY_INTEGER);
                    case BIGINT:
                        return origArray.toArray(TYPE_ARRAY_LONG);
                    case FLOAT:
                        return origArray.toArray(TYPE_ARRAY_FLOAT);
                    case DOUBLE:
                        return origArray.toArray(TYPE_ARRAY_DOUBLE);
                    default:
                        String errorMsg =
                                String.format(
                                        "Ares array type not support this type [%s] now",
                                        fieldType.getSqlType());
                        throw new FileConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
                }
            case MAP:
                HashMap<Object, Object> dataMap = new HashMap<>();
                AresDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                AresDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                HashMap<Object, Object> origDataMap = (HashMap<Object, Object>) field;
                origDataMap.forEach(
                        (key, value) ->
                                dataMap.put(
                                        resolveObject(key, keyType),
                                        resolveObject(value, valueType)));
                return dataMap;
            case BOOLEAN:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
                return field;
            case STRING:
                return field.toString();
            case TINYINT:
                return Byte.parseByte(field.toString());
            case SMALLINT:
                return Short.parseShort(field.toString());
            case NULL:
                return null;
            case BYTES:
                ByteBuffer buffer = (ByteBuffer) field;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes, 0, bytes.length);
                return bytes;
            case TIMESTAMP:
                if (field instanceof GenericData.Fixed) {
                    Binary binary =
                            Binary.fromConstantByteArray(((GenericData.Fixed) field).bytes());
                    NanoTime nanoTime = NanoTime.fromBinary(binary);
                    int julianDay = nanoTime.getJulianDay();
                    long nanosOfDay = nanoTime.getTimeOfDayNanos();
                    long timestamp =
                            (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * MILLIS_PER_DAY
                                    + nanosOfDay / NANOS_PER_MILLISECOND;
                    return new Timestamp(timestamp).toLocalDateTime();
                }
                Instant instant = Instant.ofEpochMilli((long) field);
                return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            case ROW:
                AresRowType rowType = (AresRowType) fieldType;
                Object[] objects = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    AresDataType<?> dataType = rowType.getFieldType(i);
                    objects[i] = resolveObject(((GenericRecord) field).get(i), dataType);
                }
                return new AresRow(objects);
            default:
                // do nothing
                // never got in there
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Ares not support this data type now");
        }
    }

    @Override
    public AresRowType getAresRowTypeInfo(String path) throws FileConnectorException {
        return getAresRowTypeInfo(TablePath.DEFAULT, path);
    }

    @Override
    public AresRowType getAresRowTypeInfo(TablePath tablePath, String path)
            throws FileConnectorException {
        ParquetMetadata metadata;
        try (ParquetFileReader reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        ((configuration, userGroupInformation) -> {
                            HadoopInputFile hadoopInputFile =
                                    HadoopInputFile.fromPath(new Path(path), configuration);
                            return ParquetFileReader.open(hadoopInputFile);
                        }))) {
            metadata = reader.getFooter();
        } catch (IOException e) {
            String errorMsg =
                    String.format("Create parquet reader for this file [%s] failed", path);
            throw new FileConnectorException(
                    CommonErrorCode.READER_OPERATION_FAILED, errorMsg, e);
        }
        FileMetaData fileMetaData = metadata.getFileMetaData();
        MessageType originalSchema = fileMetaData.getSchema();
        if (readColumns.isEmpty()) {
            for (int i = 0; i < originalSchema.getFieldCount(); i++) {
                readColumns.add(originalSchema.getFieldName(i));
            }
        }
        String[] fields = new String[readColumns.size()];
        AresDataType<?>[] types = new AresDataType[readColumns.size()];
        indexes = new int[readColumns.size()];
        buildColumnsWithErrorCheck(
                tablePath,
                IntStream.range(0, readColumns.size()).iterator(),
                i -> {
                    fields[i] = readColumns.get(i);
                    Type type = originalSchema.getType(fields[i]);
                    int fieldIndex = originalSchema.getFieldIndex(fields[i]);
                    indexes[i] = fieldIndex;
                    types[i] = parquetType2AresType(type, fields[i]);
                });
        aresRowType = new AresRowType(fields, types);
        aresRowTypeWithPartition = mergePartitionTypes(path, aresRowType);
        return getActualAresRowTypeInfo();
    }

    private AresDataType<?> parquetType2AresType(Type type, String name) {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32:
                    OriginalType originalType = type.asPrimitiveType().getOriginalType();
                    if (originalType == null) {
                        return BasicType.INT_TYPE;
                    }
                    switch (type.asPrimitiveType().getOriginalType()) {
                        case INT_8:
                            return BasicType.BYTE_TYPE;
                        case INT_16:
                            return BasicType.SHORT_TYPE;
                        case DATE:
                            return LocalTimeType.LOCAL_DATE_TYPE;
                        default:
                            throw CommonError.convertToAresTypeError(
                                    PARQUET, type.toString(), name);
                    }
                case INT64:
                    if (type.asPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                    }
                    return BasicType.LONG_TYPE;
                case INT96:
                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                case BINARY:
                    if (type.asPrimitiveType().getOriginalType() == null) {
                        return PrimitiveByteArrayType.INSTANCE;
                    }
                    return BasicType.STRING_TYPE;
                case FLOAT:
                    return BasicType.FLOAT_TYPE;
                case DOUBLE:
                    return BasicType.DOUBLE_TYPE;
                case BOOLEAN:
                    return BasicType.BOOLEAN_TYPE;
                case FIXED_LEN_BYTE_ARRAY:
                    if (type.getLogicalTypeAnnotation() == null) {
                        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                    }
                    String typeInfo =
                            type.getLogicalTypeAnnotation()
                                    .toString()
                                    .replaceAll(SqlType.DECIMAL.toString(), "")
                                    .replaceAll("\\(", "")
                                    .replaceAll("\\)", "");
                    String[] splits = typeInfo.split(",");
                    int precision = Integer.parseInt(splits[0]);
                    int scale = Integer.parseInt(splits[1]);
                    return new DecimalType(precision, scale);
                default:
                    throw CommonError.convertToAresTypeError("Parquet", type.toString(), name);
            }
        } else {
            LogicalTypeAnnotation logicalTypeAnnotation =
                    type.asGroupType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
                // struct type
                List<Type> fields = type.asGroupType().getFields();
                String[] fieldNames = new String[fields.size()];
                AresDataType<?>[] aresDataTypes = new AresDataType<?>[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    Type fieldType = fields.get(i);
                    AresDataType<?> aresDataType =
                            parquetType2AresType(fields.get(i), name);
                    fieldNames[i] = fieldType.getName();
                    aresDataTypes[i] = aresDataType;
                }
                return new AresRowType(fieldNames, aresDataTypes);
            } else {
                switch (logicalTypeAnnotation.toOriginalType()) {
                    case MAP:
                        GroupType groupType = type.asGroupType().getType(0).asGroupType();
                        AresDataType<?> keyType =
                                parquetType2AresType(groupType.getType(0), name);
                        AresDataType<?> valueType =
                                parquetType2AresType(groupType.getType(1), name);
                        return new MapType<>(keyType, valueType);
                    case LIST:
                        Type elementType;
                        try {
                            elementType = type.asGroupType().getType(0).asGroupType().getType(0);
                        } catch (Exception e) {
                            elementType = type.asGroupType().getType(0);
                        }
                        AresDataType<?> fieldType =
                                parquetType2AresType(elementType, name);
                        switch (fieldType.getSqlType()) {
                            case STRING:
                                return ArrayType.STRING_ARRAY_TYPE;
                            case BOOLEAN:
                                return ArrayType.BOOLEAN_ARRAY_TYPE;
                            case TINYINT:
                                return ArrayType.BYTE_ARRAY_TYPE;
                            case SMALLINT:
                                return ArrayType.SHORT_ARRAY_TYPE;
                            case INT:
                                return ArrayType.INT_ARRAY_TYPE;
                            case BIGINT:
                                return ArrayType.LONG_ARRAY_TYPE;
                            case FLOAT:
                                return ArrayType.FLOAT_ARRAY_TYPE;
                            case DOUBLE:
                                return ArrayType.DOUBLE_ARRAY_TYPE;
                            default:
                                throw CommonError.convertToAresTypeError(
                                        PARQUET, type.toString(), name);
                        }
                    default:
                        throw CommonError.convertToAresTypeError(
                                PARQUET, type.toString(), name);
                }
            }
        }
    }

    @Override
    boolean checkFileType(String path) {
        boolean checkResult;
        byte[] magic = new byte[PARQUET_MAGIC.length];
        try {
            FSDataInputStream in = hadoopFileSystemProxy.getInputStream(path);
            // try to get header information in a parquet file
            in.seek(0);
            in.readFully(magic);
            checkResult = Arrays.equals(magic, PARQUET_MAGIC);
            in.close();
            return checkResult;
        } catch (IOException e) {
            String errorMsg = String.format("Check parquet file [%s] failed", path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
    }
}
