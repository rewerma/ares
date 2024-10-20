package com.github.ares.connctor.example.sink;

import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.configuration.ReadonlyConfig;
import com.github.ares.connctor.example.state.SinkState;

import java.io.IOException;

public class ExampleSink implements AresSink<AresRow, SinkState, Object, Object> {

    AresRowType rowType;
    SinkWriter<AresRow, Object, Object> sinkWriter;

    public ExampleSink(ReadonlyConfig config, AresRowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public String getPluginName() {
        return null;
    }

    @Override
    public SinkWriter createWriter(SinkWriter.Context context) throws IOException {
        sinkWriter = new ExampleSinkWriter(context, rowType);
        return sinkWriter;
    }
}
