package com.github.ares.connector.file.ftp.config;

import com.github.ares.common.configuration.Option;
import com.github.ares.common.configuration.Options;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.ftp.system.FtpConnectionMode;

public class FtpConfigOptions extends BaseSourceConfigOptions {
    public static final Option<String> FTP_PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("FTP server password");
    public static final Option<String> FTP_USERNAME =
            Options.key("user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("FTP server username");
    public static final Option<String> FTP_HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("FTP server host");
    public static final Option<Integer> FTP_PORT =
            Options.key("port").intType().noDefaultValue().withDescription("FTP server port");
    public static final Option<FtpConnectionMode> FTP_CONNECTION_MODE =
            Options.key("connection_mode")
                    .enumType(FtpConnectionMode.class)
                    .defaultValue(FtpConnectionMode.ACTIVE_LOCAL_DATA_CONNECTION_MODE)
                    .withDescription("FTP server connection mode ");
}
