package com.github.ares.connector.file.hadoop;

import com.github.ares.common.exceptions.CommonError;
import com.github.ares.connector.file.config.HadoopConf;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HadoopFileSystemProxy implements Serializable, Closeable {

    private transient UserGroupInformation userGroupInformation;

    private transient Configuration configuration;

    private transient FileSystem fileSystem;

    private final HadoopConf hadoopConf;

    public HadoopFileSystemProxy(@NonNull HadoopConf hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public boolean fileExist(@NonNull String filePath) throws IOException {
        return getFileSystem().exists(new Path(filePath));
    }

    public void createFile(@NonNull String filePath) throws IOException {
        if (!getFileSystem().createNewFile(new Path(filePath))) {
            throw CommonError.fileOperationFailed("Ares", "create", filePath);
        }
    }

    public void deleteFile(@NonNull String filePath) throws IOException {
        Path path = new Path(filePath);
        if (getFileSystem().exists(path)) {
            if (!getFileSystem().delete(path, true)) {
                throw CommonError.fileOperationFailed("Ares", "delete", filePath);
            }
        }
    }

    public void renameFile(
            @NonNull String oldFilePath,
            @NonNull String newFilePath,
            boolean removeWhenNewFilePathExist)
            throws IOException {
        Path oldPath = new Path(oldFilePath);
        Path newPath = new Path(newFilePath);

        if (!fileExist(oldPath.toString())) {
            log.warn(
                    "rename file :["
                            + oldPath
                            + "] to ["
                            + newPath
                            + "] already finished in the last commit, skip");
            return;
        }

        if (removeWhenNewFilePathExist) {
            if (fileExist(newFilePath)) {
                getFileSystem().delete(newPath, true);
                log.info("Delete already file: {}", newPath);
            }
        }
        if (!fileExist(newPath.getParent().toString())) {
            createDir(newPath.getParent().toString());
        }

        if (getFileSystem().rename(oldPath, newPath)) {
            log.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
        } else {
            throw CommonError.fileOperationFailed(
                    "Ares", "rename", oldFilePath + " -> " + newFilePath);
        }
    }

    public void createDir(@NonNull String filePath) throws IOException {
        Path dfs = new Path(filePath);
        if (!getFileSystem().mkdirs(dfs)) {
            throw CommonError.fileOperationFailed("Ares", "create", filePath);
        }
    }

    public List<LocatedFileStatus> listFile(String path) throws IOException {
        List<LocatedFileStatus> fileList = new ArrayList<>();
        if (!fileExist(path)) {
            return fileList;
        }
        Path fileName = new Path(path);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                getFileSystem().listFiles(fileName, false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            fileList.add(locatedFileStatusRemoteIterator.next());
        }
        return fileList;
    }

    public List<Path> getAllSubFiles(@NonNull String filePath) throws IOException {
        List<Path> pathList = new ArrayList<>();
        if (!fileExist(filePath)) {
            return pathList;
        }
        Path fileName = new Path(filePath);
        FileStatus[] status = getFileSystem().listStatus(fileName);
        if (status != null) {
            for (FileStatus fileStatus : status) {
                if (fileStatus.isDirectory()) {
                    pathList.add(fileStatus.getPath());
                }
            }
        }
        return pathList;
    }

    public FileStatus[] listStatus(String filePath) throws IOException {
        return getFileSystem().listStatus(new Path(filePath));
    }

    public FileStatus getFileStatus(String filePath) throws IOException {
        return getFileSystem().getFileStatus(new Path(filePath));
    }

    public FSDataOutputStream getOutputStream(String filePath) throws IOException {
        return getFileSystem().create(new Path(filePath), true);
    }

    public FSDataInputStream getInputStream(String filePath) throws IOException {
        return getFileSystem().open(new Path(filePath));
    }

    public FileSystem getFileSystem() {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem;
    }

    @SneakyThrows
    public <T> T doWithHadoopAuth(HadoopLoginFactory.LoginFunction<T> loginFunction) {
        if (configuration == null) {
            this.configuration = createConfiguration();
        }
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            return HadoopLoginFactory.loginWithKerberos(
                    configuration,
                    hadoopConf.getKrb5Path(),
                    hadoopConf.getKerberosPrincipal(),
                    hadoopConf.getKerberosKeytabPath(),
                    loginFunction);
        }
        if (enableRemoteUser()) {
            return HadoopLoginFactory.loginWithRemoteUser(
                    configuration, hadoopConf.getRemoteUser(), loginFunction);
        }
        return loginFunction.run(configuration, UserGroupInformation.getCurrentUser());
    }

    @Override
    public void close() throws IOException {
        try {
            if (userGroupInformation != null && enableKerberos()) {
                userGroupInformation.logoutUserFromKeytab();
            }
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    @SneakyThrows
    private void initialize() {
        this.configuration = createConfiguration();
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            initializeWithKerberosLogin();
            return;
        }
        if (enableRemoteUser()) {
            initializeWithRemoteUserLogin();
            return;
        }
        this.fileSystem = org.apache.hadoop.fs.FileSystem.get(configuration);
        this.fileSystem.setWriteChecksum(false);
    }

    private Configuration createConfiguration() {
        Configuration configuration = hadoopConf.toConfiguration();
        hadoopConf.setExtraOptionsForConfiguration(configuration);
        return configuration;
    }

    private boolean enableKerberos() {
        boolean kerberosPrincipalEmpty = StringUtils.isBlank(hadoopConf.getKerberosPrincipal());
        boolean kerberosKeytabPathEmpty = StringUtils.isBlank(hadoopConf.getKerberosKeytabPath());
        if (kerberosKeytabPathEmpty && kerberosPrincipalEmpty) {
            return false;
        }
        if (!kerberosPrincipalEmpty && !kerberosKeytabPathEmpty) {
            return true;
        }
        if (kerberosPrincipalEmpty) {
            throw new IllegalArgumentException("Please set kerberosPrincipal");
        }
        throw new IllegalArgumentException("Please set kerberosKeytabPath");
    }

    private void initializeWithKerberosLogin() throws IOException, InterruptedException {
        Pair<UserGroupInformation, FileSystem> pair =
                HadoopLoginFactory.loginWithKerberos(
                        configuration,
                        hadoopConf.getKrb5Path(),
                        hadoopConf.getKerberosPrincipal(),
                        hadoopConf.getKerberosKeytabPath(),
                        (configuration, userGroupInformation) -> {
                            this.userGroupInformation = userGroupInformation;
                            this.fileSystem = FileSystem.get(configuration);
                            return Pair.of(userGroupInformation, fileSystem);
                        });
        // todo: Use a daemon thread to reloginFromTicketCache
        this.userGroupInformation = pair.getKey();
        this.fileSystem = pair.getValue();
        this.fileSystem.setWriteChecksum(false);
        log.info("Create FileSystem success with Kerberos: {}.", hadoopConf.getKerberosPrincipal());
    }

    private boolean enableRemoteUser() {
        return StringUtils.isNotBlank(hadoopConf.getRemoteUser());
    }

    private void initializeWithRemoteUserLogin() throws Exception {
        final Pair<UserGroupInformation, FileSystem> pair =
                HadoopLoginFactory.loginWithRemoteUser(
                        configuration,
                        hadoopConf.getRemoteUser(),
                        (configuration, userGroupInformation) -> {
                            final FileSystem fileSystem = FileSystem.get(configuration);
                            return Pair.of(userGroupInformation, fileSystem);
                        });
        log.info("Create FileSystem success with RemoteUser: {}.", hadoopConf.getRemoteUser());
        this.userGroupInformation = pair.getKey();
        this.fileSystem = pair.getValue();
        this.fileSystem.setWriteChecksum(false);
    }
}
