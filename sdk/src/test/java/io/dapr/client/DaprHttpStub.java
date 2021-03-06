/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT License.
 */

package io.dapr.client;

import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Stub class for DaprHttp.
 * Useful to mock as well since it provides a default constructor.
 */
public class DaprHttpStub extends DaprHttp {

    public static class ResponseStub extends DaprHttp.Response {
        public ResponseStub(byte[] body, Map<String, String> headers, int statusCode) {
            super(body, headers, statusCode);
        }
    }
    /**
     * Instantiates a stub for DaprHttp
     */
    public DaprHttpStub() {
        super(3000, null);
    }

    /**
     * {@inheritDoc}
     * @return
     */
    @Override
    public Mono<DaprHttp.Response> invokeApi(String method, String urlString, Map<String, String> urlParameters, Map<String, String> headers) {
        return Mono.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Mono<DaprHttp.Response> invokeApi(String method, String urlString, Map<String, String> urlParameters, String content, Map<String, String> headers) {
        return Mono.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Mono<DaprHttp.Response> invokeApi(String method, String urlString, Map<String, String> urlParameters, byte[] content, Map<String, String> headers) {
        return Mono.empty();
    }
}
