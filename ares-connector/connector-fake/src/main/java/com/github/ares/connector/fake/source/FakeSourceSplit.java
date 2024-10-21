package com.github.ares.connector.fake.source;

import com.github.ares.api.source.SourceSplit;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FakeSourceSplit implements SourceSplit {

    private String tableId;

    private int splitId;

    private int rowNum;

    @Override
    public String splitId() {
        return tableId + "_" + splitId;
    }
}
