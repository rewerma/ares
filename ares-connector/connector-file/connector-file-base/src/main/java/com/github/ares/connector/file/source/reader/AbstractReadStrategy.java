package com.github.ares.connector.file.source.reader;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.api.table.type.BasicType;
import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.hadoop.HadoopFileSystemProxy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public abstract class AbstractReadStrategy implements ReadStrategy {
    protected static final String[] TYPE_ARRAY_STRING = new String[0];
    protected static final Boolean[] TYPE_ARRAY_BOOLEAN = new Boolean[0];
    protected static final Byte[] TYPE_ARRAY_BYTE = new Byte[0];
    protected static final Short[] TYPE_ARRAY_SHORT = new Short[0];
    protected static final Integer[] TYPE_ARRAY_INTEGER = new Integer[0];
    protected static final Long[] TYPE_ARRAY_LONG = new Long[0];
    protected static final Float[] TYPE_ARRAY_FLOAT = new Float[0];
    protected static final Double[] TYPE_ARRAY_DOUBLE = new Double[0];
    protected static final BigDecimal[] TYPE_ARRAY_BIG_DECIMAL = new BigDecimal[0];
    protected static final LocalDate[] TYPE_ARRAY_LOCAL_DATE = new LocalDate[0];
    protected static final LocalDateTime[] TYPE_ARRAY_LOCAL_DATETIME = new LocalDateTime[0];

    protected HadoopConf hadoopConf;
    protected AresRowType aresRowType;
    protected AresRowType aresRowTypeWithPartition;
    protected Config pluginConfig;
    protected List<String> fileNames = new ArrayList<>();
    protected List<String> readPartitions = new ArrayList<>();
    protected List<String> readColumns = new ArrayList<>();
    protected boolean isMergePartition = true;
    protected long skipHeaderNumber = BaseSourceConfigOptions.SKIP_HEADER_ROW_NUMBER.defaultValue();
    protected transient boolean isKerberosAuthorization = false;
    protected HadoopFileSystemProxy hadoopFileSystemProxy;

    protected Pattern pattern;

    @Override
    public void init(HadoopConf conf) {
        this.hadoopConf = conf;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
    }

    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        this.aresRowType = aresRowType;
        this.aresRowTypeWithPartition =
                mergePartitionTypes(fileNames.get(0), aresRowType);
    }

    boolean checkFileType(String path) {
        return true;
    }

    @Override
    public List<String> getFileNamesByPath(String path, FileFormat fileFormat) throws IOException {
        ArrayList<String> fileNames = new ArrayList<>();
        FileStatus[] stats = hadoopFileSystemProxy.listStatus(path);
        for (FileStatus fileStatus : stats) {
            if (fileStatus.isDirectory()) {
                fileNames.addAll(getFileNamesByPath(fileStatus.getPath().toString(), fileFormat));
                continue;
            }
            if (fileStatus.isFile() && filterFileByPattern(fileStatus) && fileStatus.getLen() > 0) {
                // filter '_SUCCESS' file
                if (!fileStatus.getPath().getName().equals("_SUCCESS")
                        && !fileStatus.getPath().getName().startsWith(".")
                        && (fileFormat==null || fileStatus.getPath().getName().toLowerCase(Locale.ROOT).endsWith(fileFormat.getSuffix()))) {
                    String filePath = fileStatus.getPath().toString();
                    if (!readPartitions.isEmpty()) {
                        for (String readPartition : readPartitions) {
                            if (filePath.contains(readPartition)) {
                                fileNames.add(filePath);
                                this.fileNames.add(filePath);
                                break;
                            }
                        }
                    } else {
                        fileNames.add(filePath);
                        this.fileNames.add(filePath);
                    }
                }
            }
        }

        return fileNames;
    }

    @Override
    public void setPluginConfig(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        if (pluginConfig.hasPath(BaseSourceConfigOptions.PARSE_PARTITION_FROM_PATH.key())) {
            isMergePartition =
                    pluginConfig.getBoolean(
                            BaseSourceConfigOptions.PARSE_PARTITION_FROM_PATH.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.SKIP_HEADER_ROW_NUMBER.key())) {
            skipHeaderNumber =
                    pluginConfig.getLong(BaseSourceConfigOptions.SKIP_HEADER_ROW_NUMBER.key());
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_PARTITIONS.key())) {
            readPartitions.addAll(
                    pluginConfig.getStringList(BaseSourceConfigOptions.READ_PARTITIONS.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.READ_COLUMNS.key())) {
            readColumns.addAll(
                    pluginConfig.getStringList(BaseSourceConfigOptions.READ_COLUMNS.key()));
        }
        if (pluginConfig.hasPath(BaseSourceConfigOptions.FILE_FILTER_PATTERN.key())) {
            String filterPattern =
                    pluginConfig.getString(BaseSourceConfigOptions.FILE_FILTER_PATTERN.key());
            this.pattern = Pattern.compile(Matcher.quoteReplacement(filterPattern));
        }
    }

    @Override
    public AresRowType getActualAresRowTypeInfo() {
        return isMergePartition ? aresRowTypeWithPartition : aresRowType;
    }

    protected Map<String, String> parsePartitionsByPath(String path) {
        LinkedHashMap<String, String> partitions = new LinkedHashMap<>();
        Arrays.stream(path.split("/", -1))
                .filter(split -> split.contains("="))
                .map(split -> split.split("=", -1))
                .forEach(kv -> partitions.put(kv[0], kv[1]));
        return partitions;
    }

    protected AresRowType mergePartitionTypes(String path, AresRowType aresRowType) {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        if (partitionsMap.isEmpty()) {
            return aresRowType;
        }
        // get all names of partitions fields
        String[] partitionNames = partitionsMap.keySet().toArray(TYPE_ARRAY_STRING);
        // initialize data type for partition fields
        AresDataType<?>[] partitionTypes = new AresDataType<?>[partitionNames.length];
        Arrays.fill(partitionTypes, BasicType.STRING_TYPE);
        // get origin field names
        String[] fieldNames = aresRowType.getFieldNames();
        // get origin data types
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        // create new array to merge partition fields and origin fields
        String[] newFieldNames = new String[fieldNames.length + partitionNames.length];
        // create new array to merge partition fields' data type and origin fields' data type
        AresDataType<?>[] newFieldTypes =
                new AresDataType<?>[fieldTypes.length + partitionTypes.length];
        // copy origin field names to new array
        System.arraycopy(fieldNames, 0, newFieldNames, 0, fieldNames.length);
        // copy partitions field name to new array
        System.arraycopy(
                partitionNames, 0, newFieldNames, fieldNames.length, partitionNames.length);
        // copy origin field types to new array
        System.arraycopy(fieldTypes, 0, newFieldTypes, 0, fieldTypes.length);
        // copy partition field types to new array
        System.arraycopy(
                partitionTypes, 0, newFieldTypes, fieldTypes.length, partitionTypes.length);
        // return merge row type
        return new AresRowType(newFieldNames, newFieldTypes);
    }

    protected boolean filterFileByPattern(FileStatus fileStatus) {
        if (Objects.nonNull(pattern)) {
            return pattern.matcher(fileStatus.getPath().getName()).matches();
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            if (hadoopFileSystemProxy != null) {
                hadoopFileSystemProxy.close();
            }
        } catch (Exception ignore) {
        }
    }
}
