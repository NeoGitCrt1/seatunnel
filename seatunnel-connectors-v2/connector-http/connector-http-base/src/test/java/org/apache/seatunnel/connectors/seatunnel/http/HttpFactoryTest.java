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

package org.apache.seatunnel.connectors.seatunnel.http;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.seatunnel.http.sink.HttpSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.http.sink.TjswReqBodyInterceptor;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.json.JsonMapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

@Slf4j
class HttpFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new HttpSourceFactory()).optionRule());
        Assertions.assertNotNull((new HttpSinkFactory()).optionRule());
    }
    final JsonMapper  objectMapper = new JsonMapper();
    private static final TypeReference<Map<String, Object>> rowType = new TypeReference<Map<String, Object>>(){};

    @Test
    void TjswReqBodyInterceptorTest() throws  JsonProcessingException {

//        String input = "2024-09-09T01:01:01";
//        Matcher matcher = TjswReqBodyInterceptor.DATE_TIME_PATTERN.matcher(input);
//        System.out.println("matches:" + matcher.matches());

//        System.out.println(input.substring(0,input.indexOf("T")) + " " + input.substring(input.indexOf("T") + 1));
//        System.out.println(input.replaceFirst("T", " "));


        Map<String, Object> mm = new HashMap<>();

        mm.put("yyds", ImmutableList.of("a", "b"));

        Map<String, Object> mmm = new HashMap<>();

        mmm.put("yydslist", Collections.singleton( mm));


        String x = objectMapper.writeValueAsString(mm);
        System.out.println(x);


        Map<String, Object> map;
        map = objectMapper.readValue(x, rowType);

        System.out.println(map);
        log.info("map:: {}", map);


        String gps = "{\"gps\":[{\"lat\":35.415765,\"lon\":111.319395,\"positionTime\":\"2024-07-18 13:09:33\"},{\"lat\":35.41813,\"lon\":111.320222,\"positionTime\":\"2024-07-18 13:10:03\"},{\"lat\":35.428965,\"lon\":111.317962,\"positionTime\":\"2024-07-18 13:11:33\"},{\"lat\":35.432432,\"lon\":111.316787,\"positionTime\":\"2024-07-18 13:12:03\"},{\"lat\":35.432295,\"lon\":111.30727,\"positionTime\":\"2024-07-18 13:15:33\"},{\"lat\":35.449345,\"lon\":111.271912,\"positionTime\":\"2024-07-18 13:22:03\"},{\"lat\":35.450687,\"lon\":111.266035,\"positionTime\":\"2024-07-18 13:23:33\"},{\"lat\":35.460205,\"lon\":111.242775,\"positionTime\":\"2024-07-18 13:29:33\"}]}";
        Map<String, String> stringStringMap = objectMapper.readValue(gps, new TypeReference<Map<String, String>>() {
        });
        System.out.println(stringStringMap);
    }
}
