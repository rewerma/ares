
package com.github.ares.api.table.catalog;

import com.github.ares.api.table.type.AresDataType;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
@Data
@AllArgsConstructor
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class Column implements Serializable {

    /** column name. */
    protected final String name;

    /** Data type of the column. */
    // todo: use generic type
    protected final AresDataType<?> dataType;

    /**
     * Designated column's specified column size.
     *
     * <p>For numeric data, this is the maximum precision. For character/binary data, this is the
     * length in bytes.
     *
     * <p>Null is returned for data types where the scale is not applicable.
     */
    protected final Long columnLength;

    /**
     * Number of digits to right of the decimal point.
     *
     * <p>For decimal data, this is the maximum scale. For time/timestamp data, this is the maximum
     * allowed precision of the fractional seconds component.
     *
     * <p>Null is returned for data types where the scale is not applicable.
     */
    protected final Integer scale;

    /** Does the column can be null */
    protected final boolean nullable;

    // todo: use generic type
    /** The default value of the column. */
    protected final Object defaultValue;

    protected final String comment;

    /**
     * Field type in the database For example : varchar is varchar(50),DECIMAL is DECIMAL(20,5) ,
     * int is int Each database can customize the sourceType according to its own characteristics*
     */
    protected final String sourceType;

    /** your options * */
    protected final Map<String, Object> options;

    // TODO Waiting for migration to complete before remove
    @Deprecated protected boolean isUnsigned;
    @Deprecated protected boolean isZeroFill;
    @Deprecated protected Long bitLen;
    @Deprecated protected Long longColumnLength;

    protected Column(String name, AresDataType<?> dataType, Long columnLength, Integer scale) {
        this(name, dataType, columnLength, scale, true, null, null, null, null);
    }

    protected Column(
            String name,
            AresDataType<?> dataType,
            Long columnLength,
            boolean nullable,
            Object defaultValue,
            String comment) {
        this(name, dataType, columnLength, null, nullable, defaultValue, comment, null, null);
    }

    protected Column(
            String name,
            AresDataType<?> dataType,
            Long columnLength,
            Integer scale,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            Map<String, Object> options) {
        this.name = name;
        this.dataType = dataType;
        this.columnLength = columnLength;
        this.scale = scale;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.sourceType = sourceType;
        this.options = options;

        // TODO Waiting for migration to complete before remove
        this.bitLen = columnLength != null ? columnLength * 8 : 0;
        this.longColumnLength = columnLength;
        this.isUnsigned = false;
        this.isZeroFill = false;
    }

    @Deprecated
    protected Column(
            String name,
            AresDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment) {
        this(
                name,
                dataType,
                columnLength == null ? null : columnLength.longValue(),
                nullable,
                defaultValue,
                comment);
    }

    @Deprecated
    protected Column(
            String name,
            AresDataType<?> dataType,
            Integer columnLength,
            boolean nullable,
            Object defaultValue,
            String comment,
            String sourceType,
            boolean isUnsigned,
            boolean isZeroFill,
            Long bitLen,
            Long longColumnLength,
            Map<String, Object> options) {
        this.name = name;
        this.dataType = dataType;
        this.columnLength = columnLength == null ? null : columnLength.longValue();
        this.scale = null;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.sourceType = sourceType;
        this.isUnsigned = isUnsigned;
        this.isZeroFill = isZeroFill;
        this.bitLen = bitLen;
        this.longColumnLength = longColumnLength;
        this.options = options;
    }

    /**
     * Returns whether the given column is a physical column of a table; neither computed nor
     * metadata.
     */
    public abstract boolean isPhysical();

    /** Returns a copy of the column with a replaced {@link AresDataType}. */
    public abstract Column copy(AresDataType<?> newType);

    /** Returns a copy of the column. */
    public abstract Column copy();

    /** Returns a copy of the column with a replaced name. */
    public abstract Column rename(String newColumnName);
}
