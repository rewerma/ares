package com.github.ares.common.engine;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PlType implements Serializable {

    private static final long serialVersionUID = -1L;

    private InternalFieldType type;

    private Integer precision;

    private Integer scale;

    public static PlType of(InternalFieldType type) {
        return new PlType(type);
    }

    public static PlType of(InternalFieldType type, Integer precision, Integer scale) {
        return new PlType(type, precision, scale);
    }

    public PlType(InternalFieldType type) {
        this.type = type;
    }

    public PlType(InternalFieldType type, Integer precision, Integer scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
    }
}
