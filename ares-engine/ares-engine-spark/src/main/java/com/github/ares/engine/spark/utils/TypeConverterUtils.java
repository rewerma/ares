package com.github.ares.engine.spark.utils;

import com.github.ares.api.table.catalog.PrimitiveByteArrayType;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.ArrayType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.api.table.type.DecimalType;
import com.github.ares.api.table.type.LocalTimeType;
import com.github.ares.api.table.type.MapType;
import com.github.ares.api.table.type.SqlType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TypeConverterUtils {

    private static final Map<DataType, AresDataType<?>> TO_SEA_TUNNEL_TYPES =
            new HashMap<>(16);
    public static final String ROW_KIND_FIELD = "op";
    public static final String LOGICAL_TIME_TYPE_FLAG = "logical_time_type";

    static {
        TO_SEA_TUNNEL_TYPES.put(DataTypes.NullType, BasicType.VOID_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.StringType, BasicType.STRING_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.BooleanType, BasicType.BOOLEAN_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.ByteType, BasicType.BYTE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.ShortType, BasicType.SHORT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.IntegerType, BasicType.INT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.LongType, BasicType.LONG_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.FloatType, BasicType.FLOAT_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.DoubleType, BasicType.DOUBLE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.BinaryType, PrimitiveByteArrayType.INSTANCE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.DateType, LocalTimeType.LOCAL_DATE_TYPE);
        TO_SEA_TUNNEL_TYPES.put(DataTypes.TimestampType, LocalTimeType.LOCAL_DATE_TIME_TYPE);
    }

    private TypeConverterUtils() {
        throw new UnsupportedOperationException(
                "TypeConverterUtils is a utility class and cannot be instantiated");
    }

    public static DataType convert(AresDataType<?> dataType) {
        checkNotNull(dataType, "The Ares's data type is required.");
        switch (dataType.getSqlType()) {
            case NULL:
                return DataTypes.NullType;
            case STRING:
                return DataTypes.StringType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case TINYINT:
                return DataTypes.ByteType;
            case SMALLINT:
                return DataTypes.ShortType;
            case INT:
                return DataTypes.IntegerType;
            case BIGINT:
                return DataTypes.LongType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case BYTES:
                return DataTypes.BinaryType;
            case DATE:
                return DataTypes.DateType;
            case TIME:
                return DataTypes.LongType;
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case ARRAY:
                return DataTypes.createArrayType(
                        convert(((ArrayType<?, ?>) dataType).getElementType()));
            case MAP:
                MapType<?, ?> mapType = (MapType<?, ?>) dataType;
                return DataTypes.createMapType(
                        convert(mapType.getKeyType()), convert(mapType.getValueType()));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return new org.apache.spark.sql.types.DecimalType(
                        decimalType.getPrecision(), decimalType.getScale());
            case ROW:
                return convert((AresRowType) dataType);
            default:
        }
        throw new IllegalArgumentException("Unsupported Ares's data type: " + dataType);
    }

    private static StructType convert(AresRowType rowType) {
        // TODO: row kind
        StructField[] fields = new StructField[rowType.getFieldNames().length];
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            AresDataType<?> fieldType = rowType.getFieldTypes()[i];
            Metadata metadata =
                    fieldType.getSqlType() == SqlType.TIME
                            ? new MetadataBuilder().putBoolean(LOGICAL_TIME_TYPE_FLAG, true).build()
                            : Metadata.empty();

            fields[i] =
                    new StructField(rowType.getFieldNames()[i], convert(fieldType), true, metadata);
        }
        return new StructType(fields);
    }

    public static AresDataType<?> convert(DataType sparkType) {
        checkNotNull(sparkType, "The Spark's data type is required.");
        AresDataType<?> dataType = TO_SEA_TUNNEL_TYPES.get(sparkType);
        if (dataType != null) {
            return dataType;
        }
        if (sparkType instanceof org.apache.spark.sql.types.ArrayType) {
            return convert((org.apache.spark.sql.types.ArrayType) sparkType);
        }
        if (sparkType instanceof org.apache.spark.sql.types.MapType) {
            org.apache.spark.sql.types.MapType mapType =
                    (org.apache.spark.sql.types.MapType) sparkType;
            return new MapType<>(convert(mapType.keyType()), convert(mapType.valueType()));
        }
        if (sparkType instanceof org.apache.spark.sql.types.DecimalType) {
            org.apache.spark.sql.types.DecimalType decimalType =
                    (org.apache.spark.sql.types.DecimalType) sparkType;
            return new DecimalType(decimalType.precision(), decimalType.scale());
        }
        if (sparkType instanceof StructType) {
            return convert((StructType) sparkType);
        }
        throw new IllegalArgumentException("Unsupported Spark's data type: " + sparkType.sql());
    }

    private static ArrayType<?, ?> convert(org.apache.spark.sql.types.ArrayType arrayType) {
        switch (convert(arrayType.elementType()).getSqlType()) {
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
                throw new UnsupportedOperationException(
                        String.format("Unsupported Spark's array type: %s.", arrayType.sql()));
        }
    }

    private static AresRowType convert(StructType structType) {
        StructField[] structFields = structType.fields();
        String[] fieldNames = new String[structFields.length];
        AresDataType<?>[] fieldTypes = new AresDataType[structFields.length];
        for (int i = 0; i < structFields.length; i++) {
            fieldNames[i] = structFields[i].name();
            Metadata metadata = structFields[i].metadata();
            if (metadata != null
                    && metadata.contains(LOGICAL_TIME_TYPE_FLAG)
                    && metadata.getBoolean(LOGICAL_TIME_TYPE_FLAG)) {
                fieldTypes[i] = LocalTimeType.LOCAL_TIME_TYPE;
            } else {
                fieldTypes[i] = convert(structFields[i].dataType());
            }
        }
        return new AresRowType(fieldNames, fieldTypes);
    }
}
