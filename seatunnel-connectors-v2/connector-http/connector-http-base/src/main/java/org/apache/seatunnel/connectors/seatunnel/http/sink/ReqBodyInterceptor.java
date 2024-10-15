package org.apache.seatunnel.connectors.seatunnel.http.sink;

public interface ReqBodyInterceptor {
    String bodyConvert(String rawBody);
}
