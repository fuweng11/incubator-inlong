/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.common.util;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * HTTP utils
 */
@Slf4j
public class HttpUtils {

    private static final Gson GSON = new GsonBuilder().create(); // thread safe
    public static final String HTTP_PREFIX = "http://";

    /**
     * Check whether the host and port can connect
     *
     * @param host target host address
     * @param port target port
     * @param connectTimeout connect timeout
     * @param timeUnit time unit of timeout
     * @return true if connect successfully, false if connect failed
     */
    public static boolean checkConnectivity(String host, int port, int connectTimeout, TimeUnit timeUnit) {
        InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Socket socket = new Socket();
        try {
            socket.connect(socketAddress, (int) timeUnit.toMillis(connectTimeout));
            return socket.isConnected();
        } catch (IOException e) {
            log.error(String.format("%s:%s connected failed with err msg:%s", host, port, e.getMessage()));
            return false;
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                log.warn("close connection from {}:{} failed", host, port, e);
            }
        }
    }

    /**
     * Send an HTTP request by the given rest template.
     */
    public static <T> T request(RestTemplate restTemplate, String url, HttpMethod method,
            String param, HttpHeaders header, Class<T> cls) throws Exception {
        ResponseEntity<String> exchange;
        try {
            HttpEntity<String> request = new HttpEntity<>(param, header);
            log.debug("send request to {}, param {}", url, param);
            exchange = restTemplate.exchange(url, method, request, String.class);
            String body = exchange.getBody();
            HttpStatus statusCode = exchange.getStatusCode();
            if (!statusCode.is2xxSuccessful()) {
                log.error("request error for {}, status code {}, body {}", url, statusCode, body);
            }
            if (HttpStatus.TEMPORARY_REDIRECT.equals(exchange.getStatusCode())) {
                String redirectUrl = exchange.getHeaders().getFirst(HttpHeaders.LOCATION);
                if (validUrl(redirectUrl)) {
                    exchange = restTemplate.exchange(redirectUrl, method, request, String.class);
                    body = exchange.getBody();
                    statusCode = exchange.getStatusCode();
                    if (!statusCode.is2xxSuccessful()) {
                        log.error("request error for {}, status code {}, body {}", url, statusCode, body);
                    }
                    log.debug("response from {}, status code {}", url, statusCode);
                    return GSON.fromJson(exchange.getBody(), cls);
                }
            }
            log.debug("response from {}, status code {}", url, statusCode);
            return GSON.fromJson(exchange.getBody(), cls);
        } catch (RestClientException e) {
            log.error("request for {} exception {} ", url, e.getMessage());
            throw e;
        }
    }

    /**
     * Send an HTTP request by the given rest template.
     */
    public static <T> T request(RestTemplate restTemplate, String[] urls, HttpMethod method,
            String param, HttpHeaders header, Class<T> cls) throws Exception {
        ResponseEntity<String> exchange;
        Preconditions.expectNotNull(urls, ErrorCodeEnum.INVALID_PARAMETER, "url is blank");
        for (int i = 0; i < urls.length; i++) {
            try {
                HttpEntity<String> request = new HttpEntity<>(param, header);
                log.debug("send request to {}, param {}", urls[i], param);
                exchange = restTemplate.exchange(urls[i], method, request, String.class);
                if (HttpStatus.TEMPORARY_REDIRECT.equals(exchange.getStatusCode())) {
                    String redirectUrl = exchange.getHeaders().getFirst(HttpHeaders.LOCATION);
                    if (validUrl(redirectUrl)) {
                        return request(restTemplate, exchange.getHeaders().getFirst(HttpHeaders.LOCATION), method,
                                param, header, cls);
                    }
                }
                String body = exchange.getBody();
                HttpStatus statusCode = exchange.getStatusCode();
                if (!statusCode.is2xxSuccessful()) {
                    log.error("request error for {}, status code {}, body {}", urls[i], statusCode, body);
                }
                log.debug("response from {}, status code {}", urls[i], statusCode);
                return GSON.fromJson(exchange.getBody(), cls);
            } catch (RestClientException e) {
                log.error("request for {}, error, begin retry", urls[i], e);
                if (i >= (urls.length - 1)) {
                    log.error("after retry, request for {} exception {} ", urls[i], e.getMessage());
                    throw e;
                }
            }
        }
        throw new Exception(String.format("send request to %s, params %s error", urls, param));
    }

    /**
     * Send an HTTP request
     */
    public static <T> T request(RestTemplate restTemplate, String url, HttpMethod httpMethod, Object requestBody,
            HttpHeaders header, ParameterizedTypeReference<T> typeReference) {
        if (log.isDebugEnabled()) {
            log.debug("begin request to {} by request body {}", url, GSON.toJson(requestBody));
        }

        HttpEntity<Object> requestEntity = new HttpEntity<>(requestBody, header);
        ResponseEntity<T> response = restTemplate.exchange(url, httpMethod, requestEntity, typeReference);

        if (HttpStatus.TEMPORARY_REDIRECT.equals(response.getStatusCode())) {
            String redirectUrl = response.getHeaders().getFirst(HttpHeaders.LOCATION);
            if (validUrl(redirectUrl)) {
                return request(restTemplate, response.getHeaders().getFirst(HttpHeaders.LOCATION), httpMethod,
                        requestBody,
                        header, typeReference);
            }
        }
        log.debug("success request to {}, status code {}", url, response.getStatusCode());
        Preconditions.expectTrue(response.getStatusCode().is2xxSuccessful(),
                "Request failed: " + response.getBody() + ", Status code: " + response.getStatusCode());
        return response.getBody();
    }

    /**
     * Send an void HTTP request
     */
    public static void request(RestTemplate restTemplate, String url, HttpMethod httpMethod, Object requestBody,
            HttpHeaders header) {
        log.debug("begin request to {} by request body {}", url, GSON.toJson(requestBody));
        HttpEntity<Object> requestEntity = new HttpEntity<>(requestBody, header);
        ResponseEntity<String> response = restTemplate.exchange(url, httpMethod, requestEntity, String.class);
        if (HttpStatus.TEMPORARY_REDIRECT.equals(response.getStatusCode())) {
            String redirectUrl = response.getHeaders().getFirst(HttpHeaders.LOCATION);
            if (validUrl(redirectUrl)) {
                request(restTemplate, response.getHeaders().getFirst(HttpHeaders.LOCATION), httpMethod, requestBody,
                        header);
                return;
            }

        }
        log.debug("success request to {}, status code {}", url, response.getStatusCode());
        Preconditions.expectTrue(response.getStatusCode().is2xxSuccessful(),
                "Request failed: " + response.getBody() + ", Status code: " + response.getStatusCode());
    }

    /**
     * Send an void HTTP request
     */
    public static void request(RestTemplate restTemplate, String[] urls, HttpMethod httpMethod, Object requestBody,
            HttpHeaders header) {
        Preconditions.expectNotNull(urls, ErrorCodeEnum.INVALID_PARAMETER, "url is blank");
        for (int i = 0; i < urls.length; i++) {
            try {
                log.debug("begin request to {} by request body {}", urls[i], GSON.toJson(requestBody));
                HttpEntity<Object> requestEntity = new HttpEntity<>(requestBody, header);
                ResponseEntity<String> response = restTemplate.exchange(urls[i], httpMethod, requestEntity,
                        String.class);

                if (HttpStatus.TEMPORARY_REDIRECT.equals(response.getStatusCode())) {
                    String redirectUrl = response.getHeaders().getFirst(HttpHeaders.LOCATION);
                    if (validUrl(redirectUrl)) {
                        request(restTemplate, redirectUrl, httpMethod, requestBody, header);
                        return;
                    }
                }
                log.debug("success request to {}, status code {}", urls[i], response.getStatusCode());
                Preconditions.expectTrue(response.getStatusCode().is2xxSuccessful(),
                        "Request failed: " + response.getBody() + ", Status code: " + response.getStatusCode());
                return;
            } catch (Exception e) {
                log.error("request for {}, error, begin retry", urls[i], e);
                if (i >= (urls.length - 1)) {
                    log.error("after retry, request for {} exception {} ", urls[i], e.getMessage());
                    throw e;
                }
            }
        }
    }

    /**
     * Send GET request to the specified URL.
     */
    public static <T> T getRequest(RestTemplate restTemplate, String url, Map<String, Object> params,
            HttpHeaders header, ParameterizedTypeReference<T> typeReference) {
        return request(restTemplate, buildUrlWithQueryParam(url, params), HttpMethod.GET, null, header, typeReference);
    }

    /**
     * Send PUT request to the specified URL.
     */
    public static <T> T putRequest(RestTemplate restTemplate, String url, Object params, HttpHeaders header,
            ParameterizedTypeReference<T> typeReference) {
        return request(restTemplate, url, HttpMethod.PUT, params, header, typeReference);
    }

    /**
     * Send POST request to the specified URL.
     */
    public static <T> T postRequest(RestTemplate restTemplate, String url, Object params, HttpHeaders header,
            ParameterizedTypeReference<T> typeReference) {
        return request(restTemplate, url, HttpMethod.POST, params, header, typeReference);
    }

    private static String buildUrlWithQueryParam(String url, Map<String, Object> params) {
        if (params == null) {
            return url;
        }
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url);
        params.entrySet().stream().filter(e -> e.getValue() != null)
                .forEach(e -> builder.queryParam(e.getKey(), e.getValue()));
        return builder.build(false).toUriString();
    }

    private static Boolean validUrl(String url) {
        return StringUtils.isNotBlank(url) && url.startsWith(HTTP_PREFIX);
    }

}
