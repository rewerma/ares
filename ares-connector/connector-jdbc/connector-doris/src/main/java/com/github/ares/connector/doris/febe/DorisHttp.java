package com.github.ares.connector.doris.febe;

import com.github.ares.common.exceptions.AresException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

final class DorisHttp {
    private DorisHttp() {}

    static String basicAuth(String username, String password) {
        String token =
                (username == null ? "" : username) + ":" + (password == null ? "" : password);
        return "Basic "
                + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }

    static String postJson(String url, String username, String password, String body, int timeoutMs)
            throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Type", "application/json;charset=UTF-8");
        headers.put("Authorization", basicAuth(username, password));
        HttpResult result =
                execute(
                        "POST",
                        url,
                        headers,
                        body.getBytes(StandardCharsets.UTF_8),
                        timeoutMs,
                        false);
        if (result.status >= 300) {
            throw new AresException(
                    "Doris HTTP " + result.status + " from " + url + ": " + result.body);
        }
        return result.body;
    }

    static String get(String url, String username, String password, int timeoutMs)
            throws IOException {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", basicAuth(username, password));
        HttpResult result = execute("GET", url, headers, null, timeoutMs, false);
        if (result.status >= 300) {
            throw new AresException(
                    "Doris HTTP " + result.status + " from " + url + ": " + result.body);
        }
        return result.body;
    }

    static HttpResult put(
            String url, Map<String, String> headers, byte[] body, int timeoutMs, boolean redirect)
            throws IOException {
        return execute("PUT", url, headers, body, timeoutMs, redirect);
    }

    private static HttpResult execute(
            String method,
            String url,
            Map<String, String> headers,
            byte[] body,
            int timeoutMs,
            boolean followRedirect)
            throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout(timeoutMs);
        connection.setReadTimeout(timeoutMs);
        connection.setRequestMethod(method);
        for (Map.Entry<String, String> header : headers.entrySet()) {
            connection.setRequestProperty(header.getKey(), header.getValue());
        }
        if (body != null) {
            connection.setDoOutput(true);
            connection.setFixedLengthStreamingMode(body.length);
            try (OutputStream output = connection.getOutputStream()) {
                output.write(body);
            }
        }
        int status = connection.getResponseCode();
        if (followRedirect && (status == 301 || status == 302 || status == 307 || status == 308)) {
            String location = connection.getHeaderField("Location");
            connection.disconnect();
            if (location == null || location.isEmpty()) {
                throw new AresException("Doris stream load redirect has no Location header");
            }
            return execute(method, location, headers, body, timeoutMs, false);
        }
        InputStream input =
                status >= 400 ? connection.getErrorStream() : connection.getInputStream();
        String response = read(input);
        connection.disconnect();
        return new HttpResult(status, response);
    }

    private static String read(InputStream input) throws IOException {
        if (input == null) {
            return "";
        }
        try (InputStream in = input;
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int len;
            while ((len = in.read(buffer)) >= 0) {
                output.write(buffer, 0, len);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    static final class HttpResult {
        final int status;
        final String body;

        HttpResult(int status, String body) {
            this.status = status;
            this.body = body == null ? "" : body;
        }
    }
}
