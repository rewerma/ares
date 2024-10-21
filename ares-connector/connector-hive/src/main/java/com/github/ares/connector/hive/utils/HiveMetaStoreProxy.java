package com.github.ares.connector.hive.utils;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.TypesafeConfigUtils;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import com.github.ares.connector.file.hadoop.HadoopLoginFactory;
import com.github.ares.connector.hive.config.HiveConfig;
import com.github.ares.connector.hive.exception.HiveConnectorErrorCode;
import com.github.ares.connector.hive.exception.HiveConnectorException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy {
    private HiveMetaStoreClient hiveMetaStoreClient;
    private static volatile HiveMetaStoreProxy INSTANCE = null;

    private HiveMetaStoreProxy(Config config) {
        String metastoreUri = config.getString(HiveConfig.METASTORE_URI.key());

        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", metastoreUri);
            if (config.hasPath(HiveConfig.HIVE_SITE_PATH.key())) {
                String hiveSitePath = config.getString(HiveConfig.HIVE_SITE_PATH.key());
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            }
            if (HiveMetaStoreProxyUtils.enableKerberos(config)) {
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithKerberos(
                                new Configuration(),
                                TypesafeConfigUtils.getConfig(
                                        config,
                                        BaseSourceConfigOptions.KRB5_PATH.key(),
                                        BaseSourceConfigOptions.KRB5_PATH.defaultValue()),
                                config.getString(BaseSourceConfigOptions.KERBEROS_PRINCIPAL.key()),
                                config.getString(
                                        BaseSourceConfigOptions.KERBEROS_KEYTAB_PATH.key()),
                                (configuration, userGroupInformation) ->
                                        new HiveMetaStoreClient(hiveConf));
                return;
            }
            if (HiveMetaStoreProxyUtils.enableRemoteUser(config)) {
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithRemoteUser(
                                new Configuration(),
                                config.getString(BaseSourceConfigOptions.REMOTE_USER.key()),
                                (configuration, userGroupInformation) ->
                                        new HiveMetaStoreClient(hiveConf));
                return;
            }
            this.hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            String errorMsg =
                    String.format(
                            "Using this hive uris [%s] to initialize "
                                    + "hive metastore client instance failed",
                            metastoreUri);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        } catch (MalformedURLException e) {
            String errorMsg =
                    String.format(
                            "Using this hive uris [%s], hive conf [%s] to initialize "
                                    + "hive metastore client instance failed",
                            metastoreUri, config.getString(HiveConfig.HIVE_SITE_PATH.key()));
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        } catch (Exception e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED,
                    "Login form kerberos failed",
                    e);
        }
    }

    public static HiveMetaStoreProxy getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (HiveMetaStoreProxy.class) {
                if (INSTANCE == null) {
                    INSTANCE = new HiveMetaStoreProxy(config);
                }
            }
        }
        return INSTANCE;
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return hiveMetaStoreClient.getTable(dbName, tableName);
        } catch (TException e) {
            String errorMsg =
                    String.format("Get table [%s.%s] information failed", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, errorMsg, e);
        }
    }

    public void addPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            try {
                hiveMetaStoreClient.appendPartition(dbName, tableName, partition);
            } catch (AlreadyExistsException e) {
                log.warn("The partition {} are already exists", partition);
            }
        }
    }

    public void dropPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            hiveMetaStoreClient.dropPartition(dbName, tableName, partition, false);
        }
    }

    public synchronized void close() {
        if (Objects.nonNull(hiveMetaStoreClient)) {
            hiveMetaStoreClient.close();
            HiveMetaStoreProxy.INSTANCE = null;
        }
    }
}
