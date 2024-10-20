package com.github.ares.connector.file.config;

import com.github.ares.connector.file.sink.config.FileSinkConfig;
import com.github.ares.connector.file.sink.writer.ExcelWriteStrategy;
import com.github.ares.connector.file.sink.writer.JsonWriteStrategy;
import com.github.ares.connector.file.sink.writer.OrcWriteStrategy;
import com.github.ares.connector.file.sink.writer.ParquetWriteStrategy;
import com.github.ares.connector.file.sink.writer.TextWriteStrategy;
import com.github.ares.connector.file.sink.writer.WriteStrategy;
import com.github.ares.connector.file.sink.writer.XmlWriteStrategy;
import com.github.ares.connector.file.source.reader.ExcelReadStrategy;
import com.github.ares.connector.file.source.reader.JsonReadStrategy;
import com.github.ares.connector.file.source.reader.OrcReadStrategy;
import com.github.ares.connector.file.source.reader.ParquetReadStrategy;
import com.github.ares.connector.file.source.reader.ReadStrategy;
import com.github.ares.connector.file.source.reader.TextReadStrategy;
import com.github.ares.connector.file.source.reader.XmlReadStrategy;

import java.io.Serializable;

public enum FileFormat implements Serializable {
    CSV("csv") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            fileSinkConfig.setFieldDelimiter(",");
            return new TextWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new TextReadStrategy();
        }
    },
    TEXT("txt") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new TextWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new TextReadStrategy();
        }
    },
    PARQUET("parquet") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new ParquetWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new ParquetReadStrategy();
        }
    },
    ORC("orc") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new OrcWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new OrcReadStrategy();
        }
    },
    JSON("json") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new JsonWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new JsonReadStrategy();
        }
    },
    EXCEL("xlsx") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new ExcelWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new ExcelReadStrategy();
        }
    },
    XML("xml") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new XmlWriteStrategy(fileSinkConfig);
        }

        @Override
        public ReadStrategy getReadStrategy() {
            return new XmlReadStrategy();
        }
    };

    private final String suffix;

    FileFormat(String suffix) {
        this.suffix = suffix;
    }

    public String getSuffix() {
        return "." + suffix;
    }

    public ReadStrategy getReadStrategy() {
        return null;
    }

    public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
        return null;
    }
}
