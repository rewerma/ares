package com.github.ares.connector.fake.state;

import com.github.ares.connector.fake.source.FakeSourceSplit;
import java.io.Serializable;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class FakeSourceState implements Serializable {
    private final Set<FakeSourceSplit> assignedSplits;
}
