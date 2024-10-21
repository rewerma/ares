package com.github.ares.connector.console.sink;

import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connector.sink.AbstractSimpleSink;
import com.github.ares.connector.sink.AbstractSinkWriter;

import static com.github.ares.connector.console.sink.ConsoleSinkFactory.LOG_PRINT_DATA;
import static com.github.ares.connector.console.sink.ConsoleSinkFactory.LOG_PRINT_DELAY;

public class ConsoleSink extends AbstractSimpleSink<AresRow, Void>{
    private final AresRowType aresRowType;
    private final boolean isPrintData;
    private final int delayMs;

    public ConsoleSink(AresRowType aresRowType, ReadonlyConfig options) {
        this.aresRowType = aresRowType;
        this.isPrintData = options.get(LOG_PRINT_DATA);
        this.delayMs = options.get(LOG_PRINT_DELAY);
    }

    @Override
    public AbstractSinkWriter<AresRow, Void> createWriter(SinkWriter.Context context) {
        return new ConsoleSinkWriter(aresRowType, context, isPrintData, delayMs);
    }

    @Override
    public String getPluginName() {
        return "console";
    }
}
