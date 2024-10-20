package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.MapType;
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
        for (Integer integer : sinkColumnsIndexInRow) {
            String fieldName = aresRowType.getFieldName(integer);
            Object field = aresRow.getField(integer);
            recordBuilder.set(
                    fieldName.toLowerCase(),
                    resolveObject(field, aresRowType.getFieldType(integer)));
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
                return ((LocalDateTime) data)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
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
            default:
                String errorMsg =
                        String.format(
                                "Ares file connector is not supported for this data type [%s]",
                                aresDataType.getSqlType());
                throw new FileConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private Schema buildAvroSchemaWithRowType(
            AresRowType aresRowType, List<Integer> sinkColumnsIndex) {
        ArrayList<Type> types = new ArrayList<>();
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        String[] fieldNames = aresRowType.getFieldNames();
        sinkColumnsIndex.forEach(
                index -> {
                    Type type =
                            aresDataType2ParquetDataType(
                                    fieldNames[index].toLowerCase(), fieldTypes[index]);
                    types.add(type);
                });
        MessageType aresRow =
                Types.buildMessage().addFields(types.toArray(new Type[0])).named("AresRecord");
        return schemaConverter.convert(aresRow);
    }
}
