package com.github.ares.connector.hive.source;

import com.github.ares.api.common.PluginType;
import com.github.ares.api.source.AresSource;
import com.github.ares.api.table.catalog.schema.TableSchemaOptions;
import com.github.ares.api.table.type.SqlType;
import com.github.ares.com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.com.typesafe.config.ConfigFactory;
import com.github.ares.com.typesafe.config.ConfigRenderOptions;
import com.github.ares.com.typesafe.config.ConfigValueFactory;
import com.github.ares.common.configuration.CheckConfigUtil;
import com.github.ares.common.configuration.CheckResult;
import com.github.ares.common.exceptions.AresAPIErrorCode;
import com.github.ares.common.exceptions.CommonError;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.common.utils.JsonUtils;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.hadoop.source.BaseHdfsFileSource;
import com.github.ares.connector.hive.config.HiveConfig;
import com.github.ares.connector.hive.exception.HiveConnectorErrorCode;
import com.github.ares.connector.hive.exception.HiveConnectorException;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.ares.connector.file.config.BaseSourceConfigOptions.FILE_FORMAT_TYPE;
import static com.github.ares.connector.file.config.BaseSourceConfigOptions.FILE_PATH;
import static com.github.ares.connector.file.config.BaseSourceConfigOptions.TARGET_PARTITIONS;
import static com.github.ares.connector.hive.config.HiveConfig.ORC_INPUT_FORMAT_CLASSNAME;
import static com.github.ares.connector.hive.config.HiveConfig.PARQUET_INPUT_FORMAT_CLASSNAME;
import static com.github.ares.connector.hive.config.HiveConfig.TEXT_INPUT_FORMAT_CLASSNAME;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

@AutoService(AresSource.class)
public class HiveSource extends BaseHdfsFileSource {
    private Table tableInformation;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void prepare(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig, HiveConfig.METASTORE_URI.key(), HiveConfig.TABLE_NAME.key());
        if (!result.isSuccess()) {
            throw new HiveConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        result =
                CheckConfigUtil.checkAtLeastOneExists(
                        pluginConfig,
                        TableSchemaOptions.SCHEMA.key(),
                        FILE_FORMAT_TYPE.key(),
                        FILE_PATH.key(),
                        FS_DEFAULT_NAME_KEY);
        if (result.isSuccess()) {
            throw new HiveConnectorException(
                    AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "Hive source connector does not support these setting [%s]",
                            String.join(
                                    ",",
                                    TableSchemaOptions.SCHEMA.key(),
                                    FILE_FORMAT_TYPE.key(),
                                    FILE_PATH.key(),
                                    FS_DEFAULT_NAME_KEY)));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_PARTITIONS.key())) {
            // verify partition list
            List<String> partitionsList =
                    pluginConfig.getStringList(BaseSourceConfigOptions.READ_PARTITIONS.key());
            if (partitionsList.isEmpty()) {
                throw new HiveConnectorException(
                        AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "Partitions list is empty, please check");
            }
            int depth = partitionsList.get(0).replaceAll("\\\\", "/").split("/").length;
            long count =
                    partitionsList.stream()
                            .map(partition -> partition.replaceAll("\\\\", "/").split("/").length)
                            .filter(length -> length != depth)
                            .count();
            if (count > 0) {
                throw new HiveConnectorException(
                        AresAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        "Every partition that in partition list should has the same directory depth");
            }
        }
        Pair<String[], Table> tableInfo = HiveConfig.getTableInfo(pluginConfig);
        tableInformation = tableInfo.getRight();

        if (tableInformation.getPartitionKeys() != null) {
            List<String> partitionColumns = tableInformation.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
            pluginConfig =
                    pluginConfig.withValue(
                            TARGET_PARTITIONS.key(),
                            ConfigValueFactory.fromAnyRef(partitionColumns));
        }

        String inputFormat = tableInformation.getSd().getInputFormat();
        if (TEXT_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()));
        } else if (PARQUET_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
        } else if (ORC_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            pluginConfig =
                    pluginConfig.withValue(
                            FILE_FORMAT_TYPE.key(),
                            ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
        } else {
            throw new HiveConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    "Hive connector only support [text parquet orc] table now");
        }
        // Build schema from hive table information
        // Because the entrySet in typesafe config couldn't keep key-value order
        // So use jackson to keep key-value order
        Map<String, Object> schema = parseSchema(tableInformation);
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        String render = pluginConfig.root().render(options);
        ObjectNode jsonNodes = JsonUtils.parseObject(render);
        jsonNodes.putPOJO(TableSchemaOptions.SCHEMA.key(), schema);
        pluginConfig = ConfigFactory.parseString(jsonNodes.toString());

        String hdfsLocation = tableInformation.getSd().getLocation();
        try {
            URI uri = new URI(hdfsLocation);
            String path = uri.getPath();
            String defaultFs = hdfsLocation.replace(path, "");
            pluginConfig =
                    pluginConfig
                            .withValue(
                                    BaseSourceConfigOptions.FILE_PATH.key(),
                                    ConfigValueFactory.fromAnyRef(path))
                            .withValue(
                                    FS_DEFAULT_NAME_KEY, ConfigValueFactory.fromAnyRef(defaultFs));
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hdfsLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
        super.prepare(pluginConfig);
    }

    private Map<String, Object> parseSchema(Table table) {
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        LinkedHashMap<String, Object> schema = new LinkedHashMap<>();
        List<FieldSchema> cols = table.getSd().getCols();
        for (FieldSchema col : cols) {
            String name = col.getName();
            String type = col.getType();
            fields.put(name, covertHiveTypeToAresType(name, type));
        }
        schema.put("fields", fields);
        return schema;
    }

    private Object covertHiveTypeToAresType(String name, String hiveType) {
        if (hiveType.contains("varchar")) {
            return SqlType.STRING;
        }
        if (hiveType.contains("char")) {
            throw CommonError.convertToAresTypeError(getPluginName(), hiveType, name);
        }
        if (hiveType.contains("binary")) {
            return SqlType.BYTES.name();
        }
        if (hiveType.contains("struct")) {
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            int start = hiveType.indexOf("<");
            int end = hiveType.lastIndexOf(">");
            String[] columns = hiveType.substring(start + 1, end).split(",");
            for (String column : columns) {
                String[] splits = column.split(":");
                fields.put(splits[0], covertHiveTypeToAresType(splits[0], splits[1]));
            }
            return fields;
        }
        return hiveType;
    }
}
