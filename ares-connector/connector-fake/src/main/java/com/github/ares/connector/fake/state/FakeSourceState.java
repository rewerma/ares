package com.github.ares.connector.fake.state;

import com.github.ares.connector.fake.source.FakeSourceSplit;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.Set;

@Getter
@AllArgsConstructor
public class FakeSourceState implements Serializable {
    private final Set<FakeSourceSplit> assignedSplits;
}
