package com.github.ares.api.utils;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.SqlType;

public class TypeUtil {

    /**
     * Check if the data type can be converted to another data type.
     */
    public static boolean canConvert(AresDataType<?> from, AresDataType<?> to) {
        // any type can be converted to string
        if (from == to || to.getSqlType() == SqlType.STRING) {
            return true;
        }
        if (from.getSqlType() == SqlType.TINYINT) {
            return to.getSqlType() == SqlType.SMALLINT
                    || to.getSqlType() == SqlType.INT
                    || to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.SMALLINT) {
            return to.getSqlType() == SqlType.INT || to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.INT) {
            return to.getSqlType() == SqlType.BIGINT;
        }
        if (from.getSqlType() == SqlType.FLOAT) {
            return to.getSqlType() == SqlType.DOUBLE;
        }
        return false;
    }
}
