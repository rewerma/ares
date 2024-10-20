package com.github.ares.api.table.type;

import java.io.Serializable;

public interface AresDataType<T> extends Serializable {

    /** Gets the class of the type represented by this data type. */
    Class<T> getTypeClass();

    /** Gets the SQL standard type represented by this data type. */
    SqlType getSqlType();
}

