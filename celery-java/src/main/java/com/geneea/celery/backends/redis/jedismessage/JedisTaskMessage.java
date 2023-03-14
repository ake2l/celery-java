package com.geneea.celery.backends.redis.jedismessage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JedisTaskMessage {

    @JsonProperty("content-type")
    private String contentType;
    @JsonProperty("content-encoding")
    private String contentEncoding;
    private Map<String, Object> headers;
    private String body;
    private JedisMessageProperties properties;

// constructors ----------------------------------------------------------------------------------------------------

    public JedisTaskMessage(String contentType, String contentEncoding, Map<String, Object> headers, String body, JedisMessageProperties properties) {
        this.contentType = contentType;
        this.contentEncoding = contentEncoding;
        this.headers = headers;
        this.body = body;
        this.properties = properties;
    }

    public JedisTaskMessage() {
    }

    // getter/ setter --------------------------------------------------------------------------------------------------

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public String getBody() {
        return body;
    }

    public JedisMessageProperties getProperties() {
        return properties;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    // additional class ------------------------------------------------------------------------------------------------

    public static final class Builder {
        private Map<String, Object> headers;
        private byte[] body;
        private JedisMessageProperties properties;
        private String contentType;
        private String contentEncoding;
//        private String correlationId;
//        private String replyTo;
//        private Integer deliveryMode;
//        private Map<String, String> deliveryInfo;
//        private Integer priority;
//        private String bodyEncoding;
//        private String deliveryTag;

        public Builder() {
            properties = new JedisMessageProperties();
            headers = new HashMap<>();
            properties.setDeliveryInfo(new HashMap<>());
        }

        public Builder headers(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        public Builder body(byte[] body) {
            this.body = body;
            return this;
        }

        public Builder properties(JedisMessageProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder contentEncoding(String contentEncoding) {
            this.contentEncoding = contentEncoding;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.properties.setCorrelationId(correlationId);
            return this;
        }

        public Builder replyTo(String replyTo) {
            this.properties.setReplyTo(replyTo);
            return this;
        }

        public Builder deliveryMode(Integer deliveryMode) {
            this.properties.setDeliveryMode(deliveryMode);
            return this;
        }

        public Builder deliveryInfo(Map<String, String> deliveryInfo){
            this.properties.setDeliveryInfo(deliveryInfo);
            return this;
        }

        public Builder priority(Integer priority) {
            this.properties.setPriority(priority);
            return this;
        }

        public Builder bodyEncoding(String bodyEncoding){
            this.properties.setBodyEncoding(bodyEncoding);
            return this;
        }

        public Builder deliveryTag(String deliveryTag){
            this.properties.setDeliveryTag(deliveryTag);
            return this;
        }

        public JedisTaskMessage build() {

            if (properties.getCorrelationId() == null) properties.setCorrelationId("");

            if (properties.getReplyTo() == null) properties.setReplyTo("");

            if (properties.getDeliveryMode() == null) properties.setDeliveryMode(2);

            if (properties.getPriority() == null) properties.setPriority(0);

            if (properties.getBodyEncoding() == null) properties.setBodyEncoding("base64");

            if (properties.getDeliveryTag() == null) properties.setDeliveryTag("");

            if (properties.getDeliveryInfo() == null) properties.setDeliveryInfo(new HashMap<>());
            properties.getDeliveryInfo().putIfAbsent("exchange", "");
            properties.getDeliveryInfo().putIfAbsent("routing_key", "");

            byte[] encodedBody = Base64.getEncoder().encode(body);
            String encodedBodyString = new String(encodedBody);

            return new JedisTaskMessage(this.contentType, this.contentEncoding, this.headers, encodedBodyString, this.properties);
        }
    }
}
