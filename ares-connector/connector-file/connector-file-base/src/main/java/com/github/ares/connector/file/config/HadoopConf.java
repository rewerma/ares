package com.github.ares.connector.file.config;

import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

@Data
public class HadoopConf implements Serializable {
    private static final String HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";
    private static final String SCHEMA = "hdfs";
    protected Map<String, String> extraOptions = new HashMap<>();
    protected String hdfsNameKey;
    protected String hdfsSitePath;

    protected String remoteUser;

    private String krb5Path;
    protected String kerberosPrincipal;
    protected String kerberosKeytabPath;

    public HadoopConf(String hdfsNameKey) {
        this.hdfsNameKey = hdfsNameKey;
    }

    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    public String getSchema() {
        return SCHEMA;
    }

    public void setExtraOptionsForConfiguration(Configuration configuration) {
        if (!extraOptions.isEmpty()) {
            extraOptions.forEach(configuration::set);
        }
        if (hdfsSitePath != null) {
            configuration.addResource(new Path(hdfsSitePath));
        }
    }

    public Configuration toConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setBoolean(READ_INT96_AS_FIXED, true);
        configuration.setBoolean(WRITE_FIXED_AS_INT96, true);
        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, true);
        configuration.setBoolean(String.format("fs.%s.impl.disable.cache", getSchema()), true);
        configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, getHdfsNameKey());
        configuration.set(String.format("fs.%s.impl", getSchema()), getFsHdfsImpl());
        return configuration;
    }
}
