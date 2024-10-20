package com.github.ares.connctor.example.sink;

import com.github.ares.api.sink.SinkWriter;
import com.github.ares.api.sink.SupportMultiTableSinkWriter;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

public class ExampleSinkWriter implements SinkWriter<AresRow, Object, Object>,
        SupportMultiTableSinkWriter<Object> {
    AresRowType rowType;

    public ExampleSinkWriter(SinkWriter.Context context, AresRowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void write(AresRow element) throws IOException {
        System.out.println(Arrays.toString(element.getFields()));
    }

    @Override
    public Optional<Object> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {

    }

    @Override
    public void close() throws IOException {

    }
}
