package org.apache.seatunnel.connectors.seatunnel.http.sink;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class TjswResponseCallBackParser implements ResponseParser{

    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final String reportURL;
    protected final org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper objectMapper = new ObjectMapper();
    private final String apiCode;
    public TjswResponseCallBackParser(Config pluginConfig) {
        reportURL = pluginConfig.hasPath("tjsw_report_callBack_url")? pluginConfig.getString("tjsw_report_callBack_url") : null;
        apiCode = pluginConfig.getString("tjsw_service_id");
    }

    private static final Map<String, String> callBackApiOrderNoJsonPath = ImmutableMap.of(
            "postDdxx0001", "$.ddxx[0].dduuid",
            "postFpxx0005", "$.fpxx[0].dduuid"
    );

    @Override
    public void check(String reqBody, String content, int httpStatus) {
        if (reportURL == null || !callBackApiOrderNoJsonPath.containsKey(apiCode)) {
            return;
        }

        HttpPost httpPost = new HttpPost(reportURL);
        try {
            String dduuid = JsonPath.parse(reqBody).read(callBackApiOrderNoJsonPath.get(apiCode), String.class);
            StringEntity entity = new StringEntity(objectMapper.writeValueAsString(ImmutableMap.of(
                    "platformNo", "216","target", "TIAN_JIN",
                    "records", ImmutableList.of(
                            ImmutableMap.of("apiId", apiCode,
                                    "body", reqBody.substring(0,Math.min(2000, reqBody.length())),
                                    "result", content,
                                    "orderNo", dduuid
                            )
                    )
            )), ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);
        } catch (Exception e) {
            log.error("api result parese fail ", e);
            return;
        }
        try (CloseableHttpResponse resp = httpClient.execute(httpPost)){
            log.info("{} sys inner call back result {}:{}  for tjsw response {}", apiCode, httpStatus, EntityUtils.toString(resp.getEntity()), content);
        } catch (IOException e) {
            log.error("api result call back fail ", e);
        }
    }
}
