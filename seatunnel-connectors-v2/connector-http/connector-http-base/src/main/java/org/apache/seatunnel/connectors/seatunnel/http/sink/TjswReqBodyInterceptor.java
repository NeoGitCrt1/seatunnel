package org.apache.seatunnel.connectors.seatunnel.http.sink;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TjswReqBodyInterceptor implements ReqBodyInterceptor{
    protected final Config pluginConfig;
    protected final JsonMapper  objectMapper = new JsonMapper();
    public TjswReqBodyInterceptor(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
    }
    protected static final TypeReference<Map<String, Object>> rowType = new TypeReference<Map<String, Object>>(){};
    @Override
    public String bodyConvert(String rawBody) {
        Map<String, Object> map;
        try {
            map = objectMapper.readValue(rawBody, rowType);
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
