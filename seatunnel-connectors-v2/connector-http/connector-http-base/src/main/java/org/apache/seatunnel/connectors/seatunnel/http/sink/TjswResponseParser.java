package org.apache.seatunnel.connectors.seatunnel.http.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;

import java.util.Map;

@Slf4j
public class TjswResponseParser implements ResponseParser{
    @Override
    public void check(Map<String, Object> content, int httpStatus) {
        if (HttpResponse.STATUS_OK == httpStatus) {
            if (content == null) {
                log.error("api failed with empty resp ");
                return;
            }
            Object code = content.get("code");
            if (code != null && !code.toString().equals("100")) {
                log.error("api biz failed with resp of [{}]", content);
            }
        }
    }
}
