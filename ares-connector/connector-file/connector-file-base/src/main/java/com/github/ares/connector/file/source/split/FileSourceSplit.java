package com.github.ares.connector.file.source.split;

import com.github.ares.api.source.SourceSplit;
import lombok.Getter;

public class FileSourceSplit implements SourceSplit {
    private static final long serialVersionUID = -1L;

    @Getter private final String tableId;
    @Getter private final String filePath;

    public FileSourceSplit(String splitId) {
        this.filePath = splitId;
        this.tableId = null;
    }

    public FileSourceSplit(String tableId, String filePath) {
        this.tableId = tableId;
        this.filePath = filePath;
    }

    @Override
    public String splitId() {
        // In order to be compatible with the split before the upgrade, when tableId is null,
        // filePath is directly returned
        if (tableId == null) {
            return filePath;
        }
        return tableId + "_" + filePath;
    }
}
