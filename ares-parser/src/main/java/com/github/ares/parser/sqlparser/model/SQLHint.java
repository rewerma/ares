package com.github.ares.parser.sqlparser.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SQLHint implements Serializable {
    private String hintName;

    private List<String> arguments = new ArrayList<>();

    public String getHintName() {
        return hintName;
    }

    public void setHintName(String hintName) {
        this.hintName = hintName;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void setArguments(List<String> arguments) {
        this.arguments = arguments;
    }
}
