package com.github.ares.sql.function;

import java.io.Serializable;
import java.util.List;

public interface Function extends Serializable {
    Object evaluate(List<Object> args);
}
