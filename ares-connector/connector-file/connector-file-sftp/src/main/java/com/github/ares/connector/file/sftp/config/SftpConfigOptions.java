package com.github.ares.connector.file.sftp.config;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;

public class SftpConfigOptions extends BaseSourceConfigOptions {
    public static final Option<String> SFTP_PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SFTP server password");
    public static final Option<String> SFTP_USER =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("SFTP server username");
    public static final Option<String> SFTP_HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("SFTP server host");
    public static final Option<Integer> SFTP_PORT =
            Options.key("port").intType().noDefaultValue().withDescription("SFTP server port");
}
