package com.github.ares.connector.doris.febe;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class DorisSourceState implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<String> assignedSplitIds;

    public DorisSourceState(Set<String> assignedSplitIds) {
        this.assignedSplitIds = new HashSet<>(assignedSplitIds);
    }

    public Set<String> getAssignedSplitIds() {
        return assignedSplitIds;
    }
}
