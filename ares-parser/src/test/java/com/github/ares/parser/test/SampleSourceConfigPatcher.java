package com.github.ares.parser.test;

import com.github.ares.parser.datasource.SourceConfigPatcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SampleSourceConfigPatcher implements SourceConfigPatcher {

    @Override
    public Map<String, String> patchSourceConf(String datasource) {
        if (datasource.equals("mytest")) {
            Map<String, String> res = new LinkedHashMap<>();
            res.put("connector", "jdbc");
            res.put("url", "jdbc:mysql://127.0.0.1:3306/mytest");
            res.put("driver", "com.mysql.jdbc.Driver");
            res.put("username", "root");
            res.put("password", "121212");
            return res;
        }
        if (datasource.equals("mytest_orcl")) {
            Map<String, String> res = new LinkedHashMap<>();
            res.put("connector", "jdbc");
            res.put("url", "jdbc:oracle:thin:@localhost:1521:XE");
            res.put("username", "mytest");
            res.put("password", "m121212");
            return res;
        }
        if (datasource.equals("test_sftp")) {
            Map<String, String> res = new LinkedHashMap<>();
            res.put("connector", "sftp");
            res.put("host", "127.0.0.1");
            res.put("port", "22");
            res.put("username", "rewerma");
            res.put("password", "121212");
            return res;
        }
        return null;
    }

    @Override
    public Map<String, String> patchSourceConf(String datasource, Properties properties) {
        return patchSourceConf(datasource, null);
    }

    @Override
    public List<String> patchColumns(String datasource, String tableName, String sql) {
        if (datasource.equals("mytest") && ("t_user".equalsIgnoreCase(tableName) || sql != null)) {
            List<String> res = new ArrayList<>();
            res.add("id");
            res.add("name");
            res.add("c_time");
            return res;
        }
        if (datasource.equals("mytest_orcl") && ("t_user2".equalsIgnoreCase(tableName) || sql != null)) {
            List<String> res = new ArrayList<>();
            res.add("id");
            res.add("name");
            res.add("age");
            res.add("c_time");
            return res;
        }
        return null;
    }
}
