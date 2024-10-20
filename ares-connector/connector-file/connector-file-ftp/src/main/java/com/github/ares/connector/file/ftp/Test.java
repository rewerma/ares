package com.github.ares.connector.file.ftp;

import org.apache.commons.net.ftp.FTPClient;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        FTPClient client = new FTPClient();
        client.connect("localhost", 21);
        client.login("root", "121212");
        System.out.println(client.changeWorkingDirectory("/base/ares/asdf"));
    }
}
