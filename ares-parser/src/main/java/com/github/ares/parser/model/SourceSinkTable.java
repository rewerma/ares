package com.github.ares.parser.model;

import com.github.ares.parser.plan.LogicalCreateSinkTable;
import com.github.ares.parser.plan.LogicalCreateSourceTable;
import lombok.Getter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

@Getter
public class SourceSinkTable implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, LogicalCreateSourceTable> sourceTables = new LinkedHashMap<>();
    private final Map<String, LogicalCreateSinkTable> sinkTables = new LinkedHashMap<>();
}
