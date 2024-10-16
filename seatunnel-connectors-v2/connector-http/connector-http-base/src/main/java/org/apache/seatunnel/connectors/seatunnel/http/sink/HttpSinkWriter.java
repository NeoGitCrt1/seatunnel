/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.http.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class HttpSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {
    protected final HttpClientProvider httpClient;
    protected final SeaTunnelRowType seaTunnelRowType;
    protected final HttpParameter httpParameter;
    protected final SerializationSchema serializationSchema;
    protected final ResponseParser responseParser;
    protected final ReqBodyInterceptor reqBodyInterceptor;
    protected final ObjectMapper objectMapper = new ObjectMapper();

    public HttpSinkWriter(SeaTunnelRowType seaTunnelRowType, HttpParameter httpParameter) {
        this(seaTunnelRowType, httpParameter, new JsonSerializationSchema(seaTunnelRowType), null, null);
    }

    public HttpSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            SerializationSchema serializationSchema,
            ResponseParser responseParser,
            ReqBodyInterceptor reqBodyInterceptor) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.httpParameter = httpParameter;
        this.httpClient = new HttpClientProvider(httpParameter);
        this.serializationSchema = serializationSchema;
        this.responseParser = responseParser;
        this.reqBodyInterceptor = reqBodyInterceptor;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        byte[] serialize = serializationSchema.serialize(element);
        String body = new String(serialize);
        try {
            if  (reqBodyInterceptor != null) {
                body = reqBodyInterceptor.bodyConvert(body);
            }
            // only support post web hook
            HttpResponse response =
                    httpClient.doPost(httpParameter.getUrl(), httpParameter.getHeaders(), body);
            log.info(
                    "http response status code:[{}],resp content:[{}] req body [{}]",
                    response.getCode(),
                    response.getContent(),
                    body)
                    ;
            if (responseParser != null) {
                responseParser.check(response.getContent() == null || response.getContent().isEmpty()? null : objectMapper.readValue(response.getContent(), Map.class), response.getCode());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }
}
