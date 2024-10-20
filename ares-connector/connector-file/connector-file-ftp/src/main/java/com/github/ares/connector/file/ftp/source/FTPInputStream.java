package com.github.ares.connector.file.ftp.source;


import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPException;

@Private
@Unstable
public class FTPInputStream extends FSInputStream {
    InputStream wrappedStream;
    FTPClient client;
    FileSystem.Statistics stats;
    boolean closed;
    long pos;

    public FTPInputStream(InputStream stream, FTPClient client, FileSystem.Statistics stats) {
        if (stream == null) {
            throw new IllegalArgumentException("Null InputStream");
        } else if (client != null && client.isConnected()) {
            this.wrappedStream = stream;
            this.client = client;
            this.stats = stats;
            this.pos = 0L;
            this.closed = false;
        } else {
            throw new IllegalArgumentException("FTP client null or not connected");
        }
    }

    public long getPos() throws IOException {
        return this.pos;
    }

    public void seek(long pos) throws IOException {
        throw new IOException("Seek not supported");
    }

    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException("Seek not supported");
    }

    public synchronized int read() throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        } else {
            int byteRead = this.wrappedStream.read();
            if (byteRead >= 0) {
                ++this.pos;
            }

            if (this.stats != null && byteRead >= 0) {
                this.stats.incrementBytesRead(1L);
            }

            return byteRead;
        }
    }

    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        if (this.closed) {
            throw new IOException("Stream closed");
        } else {
            int result = this.wrappedStream.read(buf, off, len);
            if (result > 0) {
                this.pos += (long)result;
            }

            if (this.stats != null && result > 0) {
                this.stats.incrementBytesRead((long)result);
            }

            return result;
        }
    }

    public synchronized void close() throws IOException {
        if (!this.closed) {
            super.close();
            this.closed = true;
            if (!this.client.isConnected()) {
                throw new FTPException("Client not connected");
            } else {
                boolean cmdCompleted = this.client.completePendingCommand();
                this.client.logout();
                this.client.disconnect();
                if (!cmdCompleted) {
                    throw new FTPException("Could not complete transfer, Reply Code - " + this.client.getReplyCode());
                }
            }
        }
    }

    public boolean markSupported() {
        return false;
    }

    public void mark(int readLimit) {
    }

    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}