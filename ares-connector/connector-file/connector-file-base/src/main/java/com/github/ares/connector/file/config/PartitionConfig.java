package com.github.ares.connector.file.config;

import java.util.List;

public interface PartitionConfig {
    List<String> getPartitionFieldList();

    boolean isPartitionFieldWriteInFile();
}
