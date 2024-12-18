package com.github.ares.connector.file.sink.writer;

import com.github.ares.api.table.type.AresDataType;
import com.github.ares.api.table.type.AresRow;
import com.github.ares.api.table.type.AresRowType;
import com.github.ares.common.exceptions.CommonErrorCode;
import com.github.ares.common.utils.Constants;
import com.github.ares.common.utils.VariablesSubstitute;
import com.github.ares.connector.file.config.BaseSinkConfig;
import com.github.ares.connector.file.config.CompressFormat;
import com.github.ares.connector.file.config.FileFormat;
import com.github.ares.connector.file.config.HadoopConf;
import com.github.ares.connector.file.exception.FileConnectorException;
import com.github.ares.connector.file.hadoop.HadoopFileSystemProxy;
import com.github.ares.connector.file.sink.commit.FileCommitInfo;
import com.github.ares.connector.file.sink.config.FileSinkConfig;
import com.github.ares.connector.file.sink.state.FileSinkState;
import com.google.common.collect.Lists;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public abstract class AbstractWriteStrategy implements WriteStrategy {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final FileSinkConfig fileSinkConfig;
    protected final CompressFormat compressFormat;
    protected final List<Integer> sinkColumnsIndexInRow;
    protected String jobId;
    protected int subTaskIndex;
    protected HadoopConf hadoopConf;
    protected HadoopFileSystemProxy hadoopFileSystemProxy;
    protected String transactionId;
    /**
     * The uuid prefix to make sure same job different file sink will not conflict.
     */
    protected String uuidPrefix;

    protected String transactionDirectory;
    protected LinkedHashMap<String, String> needMoveFiles;
    protected LinkedHashMap<String, String> beingWrittenFile = new LinkedHashMap<>();
    private LinkedHashMap<String, List<String>> partitionDirAndValuesMap;
    protected AresRowType aresRowType;

    // Checkpoint id from engine is start with 1
    protected Long checkpointId = 0L;
    protected int partId = 0;
    protected int batchSize;
    protected int currentBatchSize = 0;

    public AbstractWriteStrategy(FileSinkConfig fileSinkConfig) {
        this.fileSinkConfig = fileSinkConfig;
        this.sinkColumnsIndexInRow = fileSinkConfig.getSinkColumnsIndexInRow();
        this.batchSize = fileSinkConfig.getBatchSize();
        this.compressFormat = fileSinkConfig.getCompressFormat();
    }

    /**
     * init hadoop conf
     *
     * @param conf hadoop conf
     */
    @Override
    public void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex) {
        this.hadoopConf = conf;
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(conf);
        this.jobId = jobId;
        this.subTaskIndex = subTaskIndex;
        this.uuidPrefix = uuidPrefix;
    }

    @Override
    public void write(AresRow aresRow) throws FileConnectorException {
        if (currentBatchSize >= batchSize) {
            newFilePart();
            currentBatchSize = 0;
        }
        currentBatchSize++;
    }

    public synchronized void newFilePart() {
        this.partId++;
        beingWrittenFile.clear();
        log.debug("new file part: {}", partId);
    }

    protected AresRowType buildSchemaWithRowType(
            AresRowType aresRowType, List<Integer> sinkColumnsIndex) {
        AresDataType<?>[] fieldTypes = aresRowType.getFieldTypes();
        String[] fieldNames = aresRowType.getFieldNames();
        List<String> newFieldNames = new ArrayList<>();
        List<AresDataType<?>> newFieldTypes = new ArrayList<>();
        sinkColumnsIndex.forEach(
                index -> {
                    newFieldNames.add(fieldNames[index]);
                    newFieldTypes.add(fieldTypes[index]);
                });
        return new AresRowType(
                newFieldNames.toArray(new String[0]),
                newFieldTypes.toArray(new AresDataType[0]));
    }

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @param hadoopConf hadoop conf
     * @return Configuration
     */
    @Override
    public Configuration getConfiguration(HadoopConf hadoopConf) {
        Configuration configuration = hadoopConf.toConfiguration();
        this.hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    /**
     * set aresRowTypeInfo in writer
     *
     * @param aresRowType aresRowType
     */
    @Override
    public void setAresRowTypeInfo(AresRowType aresRowType) {
        this.aresRowType = aresRowType;
    }

    /**
     * use aresRow generate partition directory
     *
     * @param aresRow aresRow
     * @return the map of partition directory
     */
    @Override
    public LinkedHashMap<String, List<String>> generatorPartitionDir(AresRow aresRow) {
        List<Integer> partitionFieldsIndexInRow = fileSinkConfig.getPartitionFieldsIndexInRow();
        LinkedHashMap<String, List<String>> partitionDirAndValuesMap = new LinkedHashMap<>(1);
        if (CollectionUtils.isEmpty(partitionFieldsIndexInRow)) {
            partitionDirAndValuesMap.put(BaseSinkConfig.NON_PARTITION, null);
            return partitionDirAndValuesMap;
        }
        List<String> partitionFieldList = fileSinkConfig.getPartitionFieldList();
        String partitionDirExpression = fileSinkConfig.getPartitionDirExpression();
        String[] keys = new String[partitionFieldList.size()];
        String[] values = new String[partitionFieldList.size()];
        for (int i = 0; i < partitionFieldList.size(); i++) {
            keys[i] = "k" + i;
            values[i] = "v" + i;
        }
        List<String> vals = new ArrayList<>(partitionFieldsIndexInRow.size());
        String partitionDir;
        if (StringUtils.isBlank(partitionDirExpression)) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                stringBuilder
                        .append(partitionFieldList.get(i))
                        .append("=")
                        .append(aresRow.getFields()[partitionFieldsIndexInRow.get(i)]);
                if (i < partitionFieldsIndexInRow.size() - 1) {
                    stringBuilder.append("/");
                }
                vals.add(aresRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = stringBuilder.toString();
        } else {
            Map<String, String> valueMap = new HashMap<>(partitionFieldList.size() * 2);
            for (int i = 0; i < partitionFieldsIndexInRow.size(); i++) {
                valueMap.put(keys[i], partitionFieldList.get(i));
                valueMap.put(
                        values[i],
                        aresRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
                vals.add(aresRow.getFields()[partitionFieldsIndexInRow.get(i)].toString());
            }
            partitionDir = VariablesSubstitute.substitute(partitionDirExpression, valueMap);
        }
        partitionDirAndValuesMap.put(partitionDir, vals);
        return partitionDirAndValuesMap;
    }

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    @Override
    public String generateFileName(String transactionId) {
        String fileNameExpression = fileSinkConfig.getFileNameExpression();
        FileFormat fileFormat = fileSinkConfig.getFileFormat();
        String suffix = fileFormat.getSuffix();
        suffix = compressFormat.getCompressCodec() + suffix;
        if (StringUtils.isBlank(fileNameExpression)) {
            return transactionId + suffix;
        }
        String timeFormat = fileSinkConfig.getFileNameTimeFormat();
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        String formattedDate = df.format(ZonedDateTime.now());
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        valuesMap.put(BaseSinkConfig.TRANSACTION_EXPRESSION, transactionId);
        String substitute =
                VariablesSubstitute.substitute(fileNameExpression, valuesMap) + "_" + partId;
        return substitute + suffix;
    }

    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    @Override
    public Optional<FileCommitInfo> prepareCommit() {
        this.finishAndCloseFile();
        LinkedHashMap<String, String> commitMap = new LinkedHashMap<>(this.needMoveFiles);
        LinkedHashMap<String, List<String>> copyMap =
                this.partitionDirAndValuesMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new ArrayList<>(e.getValue()),
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new));
        return Optional.of(new FileCommitInfo(commitMap, copyMap, transactionDirectory));
    }

    /**
     * abort prepare commit operation
     */
    @Override
    public void abortPrepare() {
        abortPrepare(transactionId);
    }

    /**
     * abort prepare commit operation using transaction directory
     *
     * @param transactionId transaction id
     */
    public void abortPrepare(String transactionId) {
        try {
            hadoopFileSystemProxy.deleteFile(getTransactionDir(transactionId));
        } catch (IOException e) {
            throw new FileConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED,
                    "Abort transaction "
                            + transactionId
                            + " error, delete transaction directory failed",
                    e);
        }
    }

    /**
     * when a checkpoint completed, file connector should begin a new transaction and generate new
     * transaction id
     *
     * @param checkpointId checkpoint id
     */
    public void beginTransaction(Long checkpointId) {
        this.checkpointId = checkpointId;
        this.transactionId = getTransactionId(checkpointId);
        this.transactionDirectory = getTransactionDir(this.transactionId);
        this.needMoveFiles = new LinkedHashMap<>();
        this.partitionDirAndValuesMap = new LinkedHashMap<>();
    }

    private String getTransactionId(Long checkpointId) {
        return "T"
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + jobId
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + uuidPrefix
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + subTaskIndex
                + BaseSinkConfig.TRANSACTION_ID_SPLIT
                + checkpointId;
    }

    /**
     * when a checkpoint was triggered, snapshot the state of connector
     *
     * @param checkpointId checkpointId
     * @return the list of states
     */
    @Override
    public List<FileSinkState> snapshotState(long checkpointId) {
        LinkedHashMap<String, List<String>> commitMap =
                this.partitionDirAndValuesMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> new ArrayList<>(e.getValue()),
                                        (e1, e2) -> e1,
                                        LinkedHashMap::new));
        ArrayList<FileSinkState> fileState =
                Lists.newArrayList(
                        new FileSinkState(
                                this.transactionId,
                                this.uuidPrefix,
                                this.checkpointId,
                                new LinkedHashMap<>(this.needMoveFiles),
                                commitMap,
                                this.getTransactionDir(transactionId)));
        this.beingWrittenFile.clear();
        this.beginTransaction(checkpointId + 1);
        return fileState;
    }

    /**
     * using transaction id generate transaction directory
     *
     * @param transactionId transaction id
     * @return transaction directory
     */
    private String getTransactionDir(@NonNull String transactionId) {
        String transactionDirectoryPrefix =
                getTransactionDirPrefix(fileSinkConfig.getTmpPath(), jobId, uuidPrefix);
        return String.join(
                File.separator, new String[]{transactionDirectoryPrefix, transactionId});
    }

    public static String getTransactionDirPrefix(String tmpPath, String jobId, String uuidPrefix) {
        String[] strings = new String[]{tmpPath, BaseSinkConfig.ARES, jobId, uuidPrefix};
        return String.join(File.separator, strings);
    }

    public String getOrCreateFilePathBeingWritten(@NonNull AresRow aresRow) {
        LinkedHashMap<String, List<String>> dataPartitionDirAndValuesMap =
                generatorPartitionDir(aresRow);
        String beingWrittenFileKey = dataPartitionDirAndValuesMap.keySet().toArray()[0].toString();
        // get filePath from beingWrittenFile
        String beingWrittenFilePath = beingWrittenFile.get(beingWrittenFileKey);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String[] pathSegments =
                    new String[]{
                            transactionDirectory, beingWrittenFileKey, generateFileName(transactionId)
                    };
            String newBeingWrittenFilePath = String.join(File.separator, pathSegments);
            beingWrittenFile.put(beingWrittenFileKey, newBeingWrittenFilePath);
            if (!BaseSinkConfig.NON_PARTITION.equals(
                    dataPartitionDirAndValuesMap.keySet().toArray()[0].toString())) {
                partitionDirAndValuesMap.putAll(dataPartitionDirAndValuesMap);
            }
            return newBeingWrittenFilePath;
        }
    }

    public String getTargetLocation(@NonNull String aresFilePath) {
        String tmpPath =
                aresFilePath.replaceAll(
                        Matcher.quoteReplacement(transactionDirectory),
                        Matcher.quoteReplacement(fileSinkConfig.getPath()));
        return tmpPath.replaceAll(
                BaseSinkConfig.NON_PARTITION + Matcher.quoteReplacement(File.separator), "");
    }

    @Override
    public long getCheckpointId() {
        return this.checkpointId;
    }

    @Override
    public FileSinkConfig getFileSinkConfig() {
        return fileSinkConfig;
    }

    @Override
    public HadoopFileSystemProxy getHadoopFileSystemProxy() {
        return hadoopFileSystemProxy;
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

    @Override
    public void truncateFiles() throws IOException {
        FileStatus[] fileStatuses = hadoopFileSystemProxy.listStatus(fileSinkConfig.getPath());
        for (FileStatus fileStat : fileStatuses) {
            if (fileStat.isFile() && fileStat.getPath().getName().toLowerCase().endsWith(
                    fileSinkConfig.getFileFormat().getSuffix().toLowerCase())) {
                hadoopFileSystemProxy.deleteFile(fileStat.getPath().toString());
            }
        }
    }
}
