package com.github.ares.parser.model;

import com.github.ares.common.engine.PlType;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Argument implements Serializable {
    private String name;
    private PlType plType;
    private String defaultVal;

    public Argument(String name, PlType plType) {
        this.name = name;
        this.plType = plType;
    }
}
