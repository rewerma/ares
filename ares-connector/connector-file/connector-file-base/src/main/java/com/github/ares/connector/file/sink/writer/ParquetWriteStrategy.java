package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.common.exceptions.AresException;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import lombok.NonNull;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParquetWriteStrategy extends AbstractWriteStrategy {
    private final LinkedHashMap<String, ParquetWriter<GenericRecord>> beingWrittenWriter;
    private AvroSchemaConverter schemaConverter;
    private Schema schema;
    private AresRowType targetColumnsType;
    public static final int[] PRECISION_TO_BYTE_COUNT = new int[38];

    static {
        for (int prec = 1; prec <= 38; prec++) {
            // Estimated number of bytes needed.
            PRECISION_TO_BYTE_COUNT[prec - 1] =
                    (int) Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
        }
    }

    public ParquetWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex) {
        super.init(conf, jobId, uuidPrefix, subTaskIndex);
        schemaConverter = new AvroSchemaConverter(getConfiguration(hadoopConf));
    }

    @Override
    public void write(@NonNull AresRow aresRow) {
        super.write(aresRow);
        String filePath = getOrCreateFilePathBeingWritten(aresRow);
        ParquetWriter<GenericRecord> writer = getOrCreateWriter(filePath);
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);

        Pair<String, Object>[] row = new Pair[fileSinkConfig.getSinkColumnList().size()];
        for (int i = 0; i < fileSinkConfig.getSinkColumnsIndexInRow().size(); i++) {
            int index = fileSinkConfig.getSinkColumnsIndexInRow().get(i);
            String fieldName = fileSinkConfig.getSinkColumnList().get(index);
            Object value = resolveObject(aresRow.getField(i), aresRowType.getFieldType(i));
            row[index] = Pair.of(fieldName, value);
        }
        for (int i = 0; i < row.length; i++) {
            Pair<String, Object> tuple2 = row[i];
            if (tuple2 != null) {
                recordBuilder.set(tuple2.getLeft(), tuple2.getRight());
            } else {
                recordBuilder.set(fileSinkConfig.getSinkColumnList().get(i), null);
            }
        }
        GenericData.Record record = recordBuilder.build();
        try {
            writer.write(record);
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("ParquetFile", "write", filePath, e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    try {
                        v.close();
                    } catch (IOException e) {
                        String errorMsg =
                                String.format(
                                        "Close file [%s] parquet writer failed, error msg: [%s]",
                                        k, e.getMessage());
                        throw new FileConnectorException(
                                CommonErrorCode.WRITER_OPERATION_FAILED, errorMsg, e);
                    }
                    needMoveFiles.put(k, getTargetLocation(k));
                });
        this.beingWrittenWriter.clear();
    }

    private ParquetWriter<GenericRecord> getOrCreateWriter(@NonNull String filePath) {
        if (schema == null) {
            schema = buildAvroSchemaWithRowType(aresRowType, sinkColumnsIndexInRow);
        }
        ParquetWriter<GenericRecord> writer = this.beingWrittenWriter.get(filePath);
        GenericData dataModel = new GenericData();
        dataModel.addLogicalTypeConversion(new Conversions.DecimalConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        if (writer == null) {
            Path path = new Path(filePath);
            // initialize the kerberos login
            return hadoopFileSystemProxy.doWithHadoopAuth(
                    (configuration, userGroupInformation) -> {
                        try {
                            HadoopOutputFile outputFile =
                                    HadoopOutputFile.fromPath(path, getConfiguration(hadoopConf));
                            ParquetWriter<GenericRecord> newWriter =
                                    AvroParquetWriter.<GenericRecord>builder(outputFile)
                                            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                                            .withDataModel(dataModel)
                                            // use parquet v1 to improve compatibility
                                            .withWriterVersion(
                                                    ParquetProperties.WriterVersion.PARQUET_1_0)
                                            .withCompressionCodec(
                                                    compressFormat.getParquetCompression())
                                            .withSchema(schema)
                                            .build();
                            this.beingWrittenWriter.put(filePath, newWriter);
                            return newWriter;
                        } catch (IOException e) {
                            String errorMsg =
                                    String.format(
                                            "Get parquet writer for file [%s] error", filePath);
                            throw new FileConnectorException(
                                    CommonErrorCode.WRITER_OPERATION_FAILED, errorMsg, e);
                        }
                    });
        }
        return writer;
    }

    private Object resolveObject(Object data, AresDataType<?> aresDataType) {
        if (data == null) {
            return null;
        }
        switch (aresDataType.getSqlType()) {
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) aresDataType).getElementType();
                ArrayList<Object> records = new ArrayList<>(((Object[]) data).length);
                for (Object object : (Object[]) data) {
                    Object resolvedObject = resolveObject(object, elementType);
                    records.add(resolvedObject);
                }
                return records;
            case MAP:
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case NULL:
            case DECIMAL:
            case DATE:
                return data;
            case TIMESTAMP:
                return ((LocalDateTime) data).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            case BYTES:
                return ByteBuffer.wrap((byte[]) data);
            case ROW:
                AresRow aresRow = (AresRow) data;
                AresDataType<?>[] fieldTypes =
                        ((AresRowType) aresDataType).getFieldTypes();
                String[] fieldNames = ((AresRowType) aresDataType).getFieldNames();
                List<Integer> sinkColumnsIndex =
                        IntStream.rangeClosed(0, fieldNames.length - 1)
                                .boxed()
                                .collect(Collectors.toList());
                Schema recordSchema =
                        buildAvroSchemaWithRowType(
                                (AresRowType) aresDataType, sinkColumnsIndex);
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
                for (int i = 0; i < fieldNames.length; i++) {
                    recordBuilder.set(
                            fieldNames[i].toLowerCase(),
                            resolveObject(aresRow.getField(i), fieldTypes[i]));
                }
                return recordBuilder.build();
            default:
                String errorMsg =
                        String.format(
                                "Ares file connector is not supported for this data type [%s]",
                                aresDataType.getSqlType());
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    public static Type aresDataType2ParquetDataType(
            String fieldName, AresDataType<?> aresDataType) {
        switch (aresDataType.getSqlType()) {
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) aresDataType).getElementType();
                return Types.optionalGroup()
                        .as(OriginalType.LIST)
                        .addField(
                                Types.repeatedGroup()
                                        .addField(
                                                aresDataType2ParquetDataType(
                                                        "array_element", elementType))
                                        .named("bag"))
                        .named(fieldName);
            case MAP:
                AresDataType<?> keyType = ((MapType<?, ?>) aresDataType).getKeyType();
                AresDataType<?> valueType = ((MapType<?, ?>) aresDataType).getValueType();
                return ConversionPatterns.mapType(
                        Type.Repetition.OPTIONAL,
                        fieldName,
                        aresDataType2ParquetDataType("key", keyType),
                        aresDataType2ParquetDataType("value", valueType));
            case STRING:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(fieldName);
            case BOOLEAN:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case TINYINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.intType(8, true))
                        .as(OriginalType.INT_8)
                        .named(fieldName);
            case SMALLINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.intType(16, true))
                        .as(OriginalType.INT_16)
                        .named(fieldName);
            case INT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DATE:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.dateType())
                        .as(OriginalType.DATE)
                        .named(fieldName);
            case BIGINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case TIMESTAMP:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .as(OriginalType.TIMESTAMP_MILLIS)
                        .named(fieldName);
            case FLOAT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DOUBLE:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DECIMAL:
                int precision = ((DecimalType) aresDataType).getPrecision();
                int scale = ((DecimalType) aresDataType).getScale();
                return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(PRECISION_TO_BYTE_COUNT[precision - 1])
                        .as(OriginalType.DECIMAL)
                        .precision(precision)
                        .scale(scale)
                        .named(fieldName);
            case BYTES:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case ROW:
                AresDataType<?>[] fieldTypes =
                        ((AresRowType) aresDataType).getFieldTypes();
                String[] fieldNames = ((AresRowType) aresDataType).getFieldNames();
                Type[] types = new Type[fieldTypes.length];
                for (int i = 0; i < fieldNames.length; i++) {
                    Type type = aresDataType2ParquetDataType(fieldNames[i], fieldTypes[i]);
                    types[i] = type;
                }
                return Types.optionalGroup().addFields(types).named(fieldName);
            case NULL:
                return null;
            default:
                String errorMsg =
                        String.format(
                                "Ares file connector is not supported for this data type [%s]",
                                aresDataType.getSqlType());
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private AresRowType getTargetColumnsType() {
        if (this.targetColumnsType != null) {
            return this.targetColumnsType;
        }
        List<String> targetColumnTypes = fileSinkConfig.getTargetColumnTypeList();
        if (targetColumnTypes == null) {
            throw new AresException("Target column types is not set");
        }
        String[] fieldNames = new String[targetColumnTypes.size()];
        AresDataType<?>[] fieldTypes = new AresDataType<?>[targetColumnTypes.size()];
        for (int i = 0; i < targetColumnTypes.size(); i++) {
            String fieldName = fileSinkConfig.getSinkColumnList().get(i);
            fieldNames[i] = fieldName;
            String targetColumnType = targetColumnTypes.get(i);
            if (targetColumnType == null) {
                continue;
            }
            if ("int".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.INT_TYPE;
            } else if ("bigint".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.LONG_TYPE;
            } else if ("smallint".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.SHORT_TYPE;
            } else if ("tinyint".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.BYTE_TYPE;
            } else if ("boolean".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.BOOLEAN_TYPE;
            } else if ("float".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.FLOAT_TYPE;
            } else if ("double".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.DOUBLE_TYPE;
            } else if (targetColumnType.toLowerCase().startsWith("decimal")) {
                int idx = targetColumnType.indexOf("(");
                int precision = 10;
                int scale = 0;
                if (idx > -1) {
                    String precisionScaleStr = targetColumnType.substring(idx + 1, targetColumnType.length() - 1);
                    String[] precisionScale = precisionScaleStr.split(",");
                    precision = Integer.parseInt(precisionScale[0].trim());
                    scale = Integer.parseInt(precisionScale[0].trim());
                }
                fieldTypes[i] = new DecimalType(precision, scale);
            } else if ("date".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = LocalTimeType.LOCAL_DATE_TYPE;
            } else if ("timestamp".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = LocalTimeType.LOCAL_DATE_TIME_TYPE;
            } else if ("string".equalsIgnoreCase(targetColumnType)) {
                fieldTypes[i] = BasicType.STRING_TYPE;
            } else if (targetColumnType.toLowerCase().startsWith("varchar")) {
                fieldTypes[i] = BasicType.STRING_TYPE;
            } else if (targetColumnType.toLowerCase().startsWith("array")) {
                fieldTypes[i] = ArrayType.STRING_ARRAY_TYPE; // TODO: support other array types
            } else if (targetColumnType.toLowerCase().startsWith("map")) {
                fieldTypes[i] = new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE); // TODO: support other map types
            } else {
                throw new AresException(String.format("Unsupported target column '%s' type: '%s'", fieldName, targetColumnType));
            }
        }
        this.targetColumnsType = new AresRowType(fieldNames, fieldTypes);
        return this.targetColumnsType;
    }

    private Schema buildAvroSchemaWithRowType(
            AresRowType aresRowType, List<Integer> sinkColumnsIndex) {
        Type[] types = new Type[fileSinkConfig.getSinkColumnList().size()];
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();

        for (int i = 0; i < sinkColumnsIndex.size(); i++) {
            int index = sinkColumnsIndex.get(i);
            types[index] = aresDataType2ParquetDataType(fileSinkConfig.getSinkColumnList().get(index).toLowerCase(), fieldTypes[i]);
        }
        for (int i = 0; i < types.length; i++) {
            if (types[i] == null) {
                AresRowType targetColumnsType = getTargetColumnsType();
                types[i] = aresDataType2ParquetDataType(fileSinkConfig.getSinkColumnList().get(i).toLowerCase(),
                        targetColumnsType.getFieldType(i));
            }
        }

        MessageType aresRow =
                Types.buildMessage().addFields(types).named("AresRecord");
        return schemaConverter.convert(aresRow);
    }
}
