package com.github.ares.connector.starrocks.febe;

import com.github.ares.api.source.SourceSplit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StarRocksSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String id;
    private final String beAddress;
    private final List<Long> tabletIds;
    private final String opaqueQueryPlan;
    private final String database;
    private final String table;

    public StarRocksSplit(
            String id,
            String beAddress,
            List<Long> tabletIds,
            String opaqueQueryPlan,
            String database,
            String table) {
        this.id = id;
        this.beAddress = beAddress;
        this.tabletIds = Collections.unmodifiableList(new ArrayList<>(tabletIds));
        this.opaqueQueryPlan = opaqueQueryPlan;
        this.database = database;
        this.table = table;
    }

    @Override
    public String splitId() {
        return id;
    }

    public String getBeAddress() {
        return beAddress;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public String getOpaqueQueryPlan() {
        return opaqueQueryPlan;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }
}
