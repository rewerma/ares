package com.github.ares.connector.file.sftp.system;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;

/** SFTP FileSystem input stream. */
public class SFTPInputStream extends FSInputStream {

    public static final String E_SEEK_NOT_SUPPORTED = "Seek not supported";
    public static final String E_CLIENT_NULL = "SFTP client null or not connected";
    public static final String E_NULL_INPUT_STREAM = "Null InputStream";
    public static final String E_STREAM_CLOSED = "Stream closed";
    public static final String E_CLIENT_NOT_CONNECTED = "Client not connected";

    private InputStream wrappedStream;
    private ChannelSftp channel;
    private FileSystem.Statistics stats;
    private boolean closed;
    private long pos;

    SFTPInputStream(InputStream stream, ChannelSftp channel, FileSystem.Statistics stats) {

        if (stream == null) {
            throw new IllegalArgumentException(E_NULL_INPUT_STREAM);
        }
        if (channel == null || !channel.isConnected()) {
            throw new IllegalArgumentException(E_CLIENT_NULL);
        }
        this.wrappedStream = stream;
        this.channel = channel;
        this.stats = stats;

        this.pos = 0;
        this.closed = false;
    }

    @Override
    public void seek(long position) throws IOException {
        throw new IOException(E_SEEK_NOT_SUPPORTED);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException(E_SEEK_NOT_SUPPORTED);
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public synchronized int read() throws IOException {
        if (closed) {
            throw new IOException(E_STREAM_CLOSED);
        }

        int byteRead = wrappedStream.read();
        if (byteRead >= 0) {
            pos++;
        }
        if (stats != null & byteRead >= 0) {
            stats.incrementBytesRead(1);
        }
        return byteRead;
    }

    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        if (closed) {
            throw new IOException(E_STREAM_CLOSED);
        }

        int result = wrappedStream.read(buf, off, len);
        if (result > 0) {
            pos += result;
        }
        if (stats != null & result > 0) {
            stats.incrementBytesRead(result);
        }

        return result;
    }

    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        wrappedStream.close();
        super.close();
        closed = true;
        if (!channel.isConnected()) {
            throw new IOException(E_CLIENT_NOT_CONNECTED);
        }

        try {
            Session session = channel.getSession();
            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            throw new IOException(StringUtils.stringifyException(e));
        }
    }
}
