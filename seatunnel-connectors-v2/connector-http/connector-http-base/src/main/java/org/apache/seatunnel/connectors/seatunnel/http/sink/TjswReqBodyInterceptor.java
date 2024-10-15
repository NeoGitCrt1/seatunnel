package org.apache.seatunnel.connectors.seatunnel.http.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TjswReqBodyInterceptor implements ReqBodyInterceptor{
    final Config pluginConfig;
    final ObjectMapper objectMapper = new ObjectMapper();

    public TjswReqBodyInterceptor(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
    }

    @Override
    public String bodyConvert(String rawBody) {
        Map map;
        try {
            map = objectMapper.readValue(rawBody, Map.class);
        } catch (JsonProcessingException e) {
            return rawBody;
        }
        Map<String, Object> res = new HashMap<>();

        res.put("service_id", pluginConfig.getString("tjsw_service_id"));
        res.put("token", pluginConfig.getString("tjsw_token"));
        res.put("system_id", pluginConfig.getString("tjsw_system_id"));
        res.put("sign", pluginConfig.getString("tjsw_sign"));
        res.put("timestamp", pluginConfig.getString("tjsw_timestamp"));
        res.put("version", pluginConfig.getString("tjsw_version"));
        res.put("charset", pluginConfig.getString("tjsw_charset"));
        res.put("seq", UUID.randomUUID().toString().replaceAll("-", ""));
        res.put(pluginConfig.getString("tjsw_method"), Collections.singleton( map));
        try {
            return objectMapper.writeValueAsString(res);
        } catch (JsonProcessingException e) {
            return rawBody;
        }
    }
}
