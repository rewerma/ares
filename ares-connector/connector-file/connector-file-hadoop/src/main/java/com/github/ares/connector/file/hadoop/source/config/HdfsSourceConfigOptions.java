package com.github.ares.connector.file.hadoop.source.config;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;

public class HdfsSourceConfigOptions extends BaseSourceConfigOptions {
    public static final Option<String> DEFAULT_FS =
            Options.key(FS_DEFAULT_NAME_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HDFS namenode host");
}
