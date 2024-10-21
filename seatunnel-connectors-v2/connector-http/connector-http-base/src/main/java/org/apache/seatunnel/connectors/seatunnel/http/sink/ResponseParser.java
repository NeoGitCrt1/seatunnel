package org.apache.seatunnel.connectors.seatunnel.http.sink;

import java.util.Map;

public interface ResponseParser {

    void check(String reqBody, String respContent, int httpStatus);
}
