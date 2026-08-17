package com.github.ares.connector.hive.utils;

import com.github.ares.com.typesafe.config.Config;
import com.github.ares.connector.file.config.BaseSourceConfigOptions;
import lombok.experimental.UtilityClass;

@UtilityClass
public class HiveMetaStoreProxyUtils {

    public boolean enableKerberos(Config config) {
        boolean kerberosPrincipalEmpty =
                config.hasPath(BaseSourceConfigOptions.KERBEROS_PRINCIPAL.key());
        boolean kerberosKeytabPathEmpty =
                config.hasPath(BaseSourceConfigOptions.KERBEROS_KEYTAB_PATH.key());
        if (kerberosKeytabPathEmpty && kerberosPrincipalEmpty) {
            return true;
        }
        if (!kerberosPrincipalEmpty && !kerberosKeytabPathEmpty) {
            return false;
        }
        if (kerberosPrincipalEmpty) {
            throw new IllegalArgumentException("Please set kerberosPrincipal");
        }
        throw new IllegalArgumentException("Please set kerberosKeytabPath");
    }

    public boolean enableRemoteUser(Config config) {
        return config.hasPath(BaseSourceConfigOptions.REMOTE_USER.key());
    }
}
