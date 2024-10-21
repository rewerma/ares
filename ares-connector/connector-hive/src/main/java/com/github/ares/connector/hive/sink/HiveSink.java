package com.github.ares.connector.hive.sink;

import com.github.ares.api.common.PluginType;
import com.github.ares.api.sink.AresSink;
import com.github.ares.api.sink.SinkAggregatedCommitter;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.com.typesafe.config.ConfigValueFactory;
import com.github.ares.common.configuration.CheckConfigUtil;
import com.github.ares.common.configuration.CheckResult;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.hadoop.sink.BaseHdfsFileSink;
import com.github.ares.connector.file.sink.commit.FileAggregatedCommitInfo;
import com.github.ares.connector.file.sink.commit.FileCommitInfo;
import com.github.ares.connector.hive.commit.HiveSinkAggregatedCommitter;
import com.github.ares.connector.hive.config.HiveConfig;
import com.github.ares.connector.hive.exception.HiveConnectorErrorCode;
import com.github.ares.connector.hive.exception.HiveConnectorException;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.ares.connector.file.config.BaseSinkConfig.FIELD_DELIMITER;
import static com.github.ares.connector.file.config.BaseSinkConfig.FILE_FORMAT_TYPE;
import static com.github.ares.connector.file.config.BaseSinkConfig.FILE_NAME_EXPRESSION;
import static com.github.ares.connector.file.config.BaseSinkConfig.FILE_PATH;
import static com.github.ares.connector.file.config.BaseSinkConfig.HAVE_PARTITION;
import static com.github.ares.connector.file.config.BaseSinkConfig.IS_PARTITION_FIELD_WRITE_IN_FILE;
import static com.github.ares.connector.file.config.BaseSinkConfig.PARTITION_BY;
import static com.github.ares.connector.file.config.BaseSinkConfig.PARTITION_DIR_EXPRESSION;
import static com.github.ares.connector.file.config.BaseSinkConfig.ROW_DELIMITER;
import static com.github.ares.connector.file.config.BaseSinkConfig.SINK_COLUMNS;
import static com.github.ares.connector.file.config.BaseSinkConfig.TARGET_COLUMN_TYPES;
import static com.github.ares.connector.hive.config.HiveConfig.METASTORE_URI;
import static com.github.ares.connector.hive.config.HiveConfig.ORC_OUTPUT_FORMAT_CLASSNAME;
import static com.github.ares.connector.hive.config.HiveConfig.PARQUET_OUTPUT_FORMAT_CLASSNAME;
import static com.github.ares.connector.hive.config.HiveConfig.TABLE_NAME;
import static com.github.ares.connector.hive.config.HiveConfig.TEXT_OUTPUT_FORMAT_CLASSNAME;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

@AutoService(AresSink.class)
public class HiveSink extends BaseHdfsFileSink {
    private String dbName;
    private String tableName;
    private Table tableInformation;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(pluginConfig, METASTORE_URI.key(), TABLE_NAME.key());
        if (!result.isSuccess()) {
            throw new HiveConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        result =
                CheckConfigUtil.checkAtLeastOneExists(
                        pluginConfig,
                        FILE_FORMAT_TYPE.key(),
                        FILE_PATH.key(),
                        FIELD_DELIMITER.key(),
                        ROW_DELIMITER.key(),
                        IS_PARTITION_FIELD_WRITE_IN_FILE.key(),
                        PARTITION_DIR_EXPRESSION.key(),
                        HAVE_PARTITION.key(),
                        SINK_COLUMNS.key(),
                        PARTITION_BY.key());
        if (result.isSuccess()) {
            throw new HiveConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Hive sink connector does not support these setting [%s]",
                            String.join(
                                    ",",
                                    FILE_FORMAT_TYPE.key(),
                                    FILE_PATH.key(),
                                    FIELD_DELIMITER.key(),
                                    ROW_DELIMITER.key(),
                                    IS_PARTITION_FIELD_WRITE_IN_FILE.key(),
                                    PARTITION_DIR_EXPRESSION.key(),
                                    HAVE_PARTITION.key(),
                                    SINK_COLUMNS.key(),
                                    PARTITION_BY.key())));
        }
        Pair<String[], Table> tableInfo = HiveConfig.getTableInfo(pluginConfig);
        dbName = tableInfo.getLeft()[0];
        tableName = tableInfo.getLeft()[1];
        tableInformation = tableInfo.getRight();
        List<String> sinkFields =
                tableInformation.getSd().getCols().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        List<String> partitionKeys =
                tableInformation.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        sinkFields.addAll(partitionKeys);

        List<String> sinkFieldTypes = tableInformation.getSd().getCols().stream().map(FieldSchema::getType).collect(Collectors.toList());

        String outputFormat = tableInformation.getSd().getOutputFormat();
        if (TEXT_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            Map<String, String> parameters =
                    tableInformation.getSd().getSerdeInfo().getParameters();
            pluginConfig =
                    pluginConfig
                            .withValue(
                                    FILE_FORMAT_TYPE.key(),
                                    ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                            .withValue(
                                    FIELD_DELIMITER.key(),
                                    ConfigValueFactory.fromAnyRef(parameters.get("field.delim")))
                            .withValue(
                                    ROW_DELIMITER.key(),
                                    ConfigValueFactory.fromAnyRef(parameters.get("line.delim")));
        } else if (PARQUET_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
        } else if (ORC_OUTPUT_FORMAT_CLASSNAME.equals(outputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
        } else {
            throw new HiveConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Hive connector only support [text parquet orc] table now");
        }
        pluginConfig =
                pluginConfig
                        .withValue(
                                IS_PARTITION_FIELD_WRITE_IN_FILE.key(),
                                ConfigValueFactory.fromAnyRef(false))
                        .withValue(
                                FILE_NAME_EXPRESSION.key(),
                                ConfigValueFactory.fromAnyRef("${transactionId}"))
                        .withValue(
                                FILE_PATH.key(),
                                ConfigValueFactory.fromAnyRef(
                                        tableInformation.getSd().getLocation()))
                        .withValue(SINK_COLUMNS.key(), ConfigValueFactory.fromAnyRef(sinkFields))
                        .withValue(TARGET_COLUMN_TYPES.key(), ConfigValueFactory.fromAnyRef(sinkFieldTypes))
                        .withValue(
                                PARTITION_BY.key(), ConfigValueFactory.fromAnyRef(partitionKeys));
        String hdfsLocation = tableInformation.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            hadoopConf = new HadoopConf(hdfsLocation.replace(path, ""));
            pluginConfig =
                    pluginConfig
                            .withValue(FILE_PATH.key(), ConfigValueFactory.fromAnyRef(path))
                            .withValue(
                                    FS_DEFAULT_NAME_KEY,
                                    ConfigValueFactory.fromAnyRef(hadoopConf.getHdfsNameKey()));
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
        this.pluginConfig = pluginConfig;
        super.prepare(pluginConfig);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
    createAggregatedCommitter() {
        return Optional.of(
                new HiveSinkAggregatedCommitter(pluginConfig, dbName, tableName, hadoopConf));
    }
}
