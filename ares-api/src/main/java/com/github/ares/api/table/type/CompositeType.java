package com.github.ares.api.table.type;

import java.util.List;

public interface CompositeType<T> extends AresDataType<T> {
    List<AresDataType<?>> getChildren();
}
