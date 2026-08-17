package com.github.ares.connector.hive.utils;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.common.configuration.TypesafeConfigUtils;
import com.github.ares.common.utils.PluginClassLoader;
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
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HiveMetaStoreProxy {
    private static final Map<CacheKey, HiveMetaStoreProxy> INSTANCES = new ConcurrentHashMap<>();

    private final CacheKey cacheKey;
    private final Config config;
    private HiveMetaStoreClient hiveMetaStoreClient;

    private HiveMetaStoreProxy(CacheKey cacheKey, Config config) {
        this.cacheKey = cacheKey;
        this.config = config;
        String metastoreUri = config.getString(HiveConfig.METASTORE_URI.key());

        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", metastoreUri);
            if (config.hasPath(HiveConfig.HIVE_SITE_PATH.key())) {
                String hiveSitePath = config.getString(HiveConfig.HIVE_SITE_PATH.key());
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            }
            if (HiveMetaStoreProxyUtils.enableKerberos(config)) {
                Configuration loginConf = createLoginConfiguration(config, hiveConf);
                loginConf.set("hadoop.security.authentication", "kerberos");
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithKerberos(
                                loginConf,
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
                Configuration loginConf = createLoginConfiguration(config, hiveConf);
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithRemoteUser(
                                loginConf,
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
                    "Login from kerberos failed",
                    e);
        }
    }

    public static HiveMetaStoreProxy getInstance(Config config) {
        CacheKey cacheKey = CacheKey.from(config);
        return PluginClassLoader.callWithContextClassLoader(
                pluginClassLoader(),
                () ->
                        INSTANCES.computeIfAbsent(
                                cacheKey, key -> new HiveMetaStoreProxy(key, config)));
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        return PluginClassLoader.callWithContextClassLoader(
                pluginClassLoader(),
                () -> {
                    try {
                        return getTableInternal(dbName, tableName);
                    } catch (TException e) {
                        String errorMsg =
                                String.format(
                                        "Get table [%s.%s] information failed", dbName, tableName);
                        throw new HiveConnectorException(
                                HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED,
                                errorMsg,
                                e);
                    }
                });
    }

    public void addPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader());
            for (String partition : partitions) {
                try {
                    hiveMetaStoreClient.appendPartition(dbName, tableName, partition);
                } catch (AlreadyExistsException e) {
                    log.warn("The partition {} are already exists", partition);
                }
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    public void dropPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginClassLoader());
            for (String partition : partitions) {
                hiveMetaStoreClient.dropPartition(dbName, tableName, partition, false);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private Table getTableInternal(String dbName, String tableName) throws TException {
        String catalogName =
                TypesafeConfigUtils.getConfig(
                        config,
                        HiveConfig.CATALOG_NAME.key(),
                        HiveConfig.CATALOG_NAME.defaultValue());
        if ("hive".equalsIgnoreCase(catalogName)) {
            return hiveMetaStoreClient.getTable(dbName, tableName);
        }
        try {
            Method getTableWithCatalog =
                    hiveMetaStoreClient
                            .getClass()
                            .getMethod("getTable", String.class, String.class, String.class);
            return (Table) getTableWithCatalog.invoke(hiveMetaStoreClient, catalogName, dbName, tableName);
        } catch (NoSuchMethodException e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED,
                    String.format(
                            "catalog_name [%s] requires Hive 3 metastore client", catalogName),
                    e);
        } catch (ReflectiveOperationException e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED,
                    String.format(
                            "Get table [%s.%s.%s] information failed",
                            catalogName, dbName, tableName),
                    e);
        }
    }

    public synchronized void close() {
        PluginClassLoader.runWithContextClassLoader(
                pluginClassLoader(),
                () -> {
                    if (Objects.nonNull(hiveMetaStoreClient)) {
                        hiveMetaStoreClient.close();
                        hiveMetaStoreClient = null;
                        INSTANCES.remove(cacheKey);
                    }
                });
    }

    private static ClassLoader pluginClassLoader() {
        return HiveMetaStoreProxy.class.getClassLoader();
    }

    private static Configuration createLoginConfiguration(Config config, HiveConf hiveConf)
            throws MalformedURLException {
        Configuration loginConf = new Configuration(hiveConf);
        if (config.hasPath(BaseSourceConfigOptions.HDFS_SITE_PATH.key())) {
            String hdfsSitePath =
                    config.getString(BaseSourceConfigOptions.HDFS_SITE_PATH.key());
            loginConf.addResource(new File(hdfsSitePath).toURI().toURL());
        }
        return loginConf;
    }

    private static final class CacheKey {
        private final String metastoreUri;
        private final String hiveSitePath;
        private final String catalogName;
        private final String kerberosPrincipal;
        private final String remoteUser;

        private CacheKey(
                String metastoreUri,
                String hiveSitePath,
                String catalogName,
                String kerberosPrincipal,
                String remoteUser) {
            this.metastoreUri = metastoreUri;
            this.hiveSitePath = hiveSitePath;
            this.catalogName = catalogName;
            this.kerberosPrincipal = kerberosPrincipal;
            this.remoteUser = remoteUser;
        }

        private static CacheKey from(Config config) {
            String metastoreUri = config.getString(HiveConfig.METASTORE_URI.key());
            String hiveSitePath =
                    config.hasPath(HiveConfig.HIVE_SITE_PATH.key())
                            ? config.getString(HiveConfig.HIVE_SITE_PATH.key())
                            : "";
            String catalogName =
                    TypesafeConfigUtils.getConfig(
                            config,
                            HiveConfig.CATALOG_NAME.key(),
                            HiveConfig.CATALOG_NAME.defaultValue());
            String kerberosPrincipal =
                    config.hasPath(BaseSourceConfigOptions.KERBEROS_PRINCIPAL.key())
                            ? config.getString(BaseSourceConfigOptions.KERBEROS_PRINCIPAL.key())
                            : "";
            String remoteUser =
                    config.hasPath(BaseSourceConfigOptions.REMOTE_USER.key())
                            ? config.getString(BaseSourceConfigOptions.REMOTE_USER.key())
                            : "";
            return new CacheKey(
                    metastoreUri, hiveSitePath, catalogName, kerberosPrincipal, remoteUser);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(metastoreUri, cacheKey.metastoreUri)
                    && Objects.equals(hiveSitePath, cacheKey.hiveSitePath)
                    && Objects.equals(catalogName, cacheKey.catalogName)
                    && Objects.equals(kerberosPrincipal, cacheKey.kerberosPrincipal)
                    && Objects.equals(remoteUser, cacheKey.remoteUser);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    metastoreUri, hiveSitePath, catalogName, kerberosPrincipal, remoteUser);
        }
    }
}
