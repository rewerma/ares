package com.github.ares.spark.connector.serialization;

import com.github.ares.api.source.Collector;
import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.common.engine.Handover;
import com.github.ares.common.exceptions.AresException;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


public class InternalRowCollector implements Collector<AresRow> {
    private final Handover<InternalRow> handover;
    private final Object checkpointLock;
    private final AtomicLong collectTotalCount;
    private final Map<String, Object> envOptions;
    private volatile boolean emptyThisPollNext;

    private InternalRowConverter rowSerialization;

    public InternalRowCollector(
            Handover<InternalRow> handover,
            Object checkpointLock,
            AresDataType<?> dataType,
            Map<String, String> envOptionsInfo) {
        this.handover = handover;
        this.checkpointLock = checkpointLock;
        this.rowSerialization = new InternalRowConverter(dataType);
        this.collectTotalCount = new AtomicLong(0);
        this.envOptions = (Map) envOptionsInfo;
    }

    @Override
    public void collect(AresRow aresRow) {
        try {
            synchronized (checkpointLock) {
                handover.produce(rowSerialization.convert(aresRow));
            }
            collectTotalCount.incrementAndGet();
            emptyThisPollNext = false;
        } catch (Exception e) {
            throw new AresException(e);
        }
    }

    public void resetRowType(AresDataType<?> dataType) {
        this.rowSerialization = new InternalRowConverter(dataType);
    }

    public long collectTotalCount() {
        return collectTotalCount.get();
    }

    @Override
    public Object getCheckpointLock() {
        return this.checkpointLock;
    }

    @Override
    public boolean isEmptyThisPollNext() {
        return emptyThisPollNext;
    }

    @Override
    public void resetEmptyThisPollNext() {
        this.emptyThisPollNext = true;
    }
}
