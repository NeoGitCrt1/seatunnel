package org.apache.seatunnel.connectors.seatunnel.http.sink;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

@Slf4j
public class TjswResponseAlertParser implements ResponseParser{

    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final String reportURL;
    private final String reportSec;
    private final List<String> reportAt;
    protected final ObjectMapper objectMapper = new ObjectMapper();
    private final String apiCode;
    public TjswResponseAlertParser(Config pluginConfig) {
        reportURL = pluginConfig.getString("tjsw_report_dingtalk_url");
        reportSec = pluginConfig.hasPathOrNull("tjsw_report_dingtalk_sec") ? "" : pluginConfig.getString("tjsw_report_dingtalk_sec");
        reportAt = pluginConfig.hasPathOrNull("tjsw_report_dingtalk_at") ? new ArrayList<>() : pluginConfig.getStringList("tjsw_report_dingtalk_at");
        apiCode = pluginConfig.getString("tjsw_service_id");
    }

    @Override
    public void check(String reqBody, String respContent, int httpStatus) {
        try {
            String respCode = (httpStatus != HttpStatus.SC_OK || respContent == null || respContent.isEmpty()) ?
                    "APIFAIL":
                    JsonPath.parse(respContent).read("$.code", String.class);

            if ("100".equals(respCode)) {
                return;
            }
        } catch (PathNotFoundException pathNotFoundException) {
            // ignore
        }


        String msg = "天津税务上报接口: " + apiCode + "\n业务失败: " + respContent + "\n部分请求体: " + reqBody.substring(0, 100);
        String dingUrl = this.reportURL;
        //组装请求内容
        //消息内容
        Map<String, Object> contentMap = new HashMap<>();
        contentMap.put("content", msg);
        //通知人
        Map<String, Object> atMap = new HashMap<>();
        //1.是否通知所有人
        atMap.put("isAtAll", false);
        //2.通知具体人的手机号码列表
        atMap.put("atMobiles", reportAt);

        Map<String, Object> reqMap = new HashMap<>();
        reqMap.put("msgtype", "text");
        reqMap.put("text", contentMap);
        reqMap.put("at", atMap);

        //推送消息（http请求）
        final HttpPost httpPost = new HttpPost(dingUrl + secretPart(reportSec));
        httpPost.setHeader("content-type", "application/json;charset=UTF-8");
        try {
            httpPost.setEntity(new ByteArrayEntity(objectMapper.writeValueAsString(reqMap).getBytes(StandardCharsets.UTF_8)));
        } catch (JsonProcessingException e) {
            log.error("dingtalk alert error", e);
        }
        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {

        } catch (IOException e) {
            log.error("dingtalk alert error", e);
        }

    }


    private static String secretPart(String secret) {
        if (secret == null || secret.isEmpty()) {
            return "";
        }
        Long timestamp = System.currentTimeMillis();

        String stringToSign = timestamp + "\n" + secret;
        Mac mac;
        try {
            mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(UTF8), "HmacSHA256"));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }

        byte[] signData = mac.doFinal(stringToSign.getBytes(UTF8));
        String sign;
        try {
            sign = URLEncoder.encode(new String(Base64.encodeBase64(signData)),UTF8.toString());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return "&timestamp=" + timestamp + "&sign=" + sign;
    }

    static final Charset UTF8 = Charset.forName("UTF-8");
}
