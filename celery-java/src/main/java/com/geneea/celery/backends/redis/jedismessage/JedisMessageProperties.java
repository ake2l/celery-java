package com.geneea.celery.backends.redis.jedismessage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JedisMessageProperties {
    @JsonProperty("correlation_id")
    private String correlationId;

    @JsonProperty("reply_to")
    private String replyTo;

    @JsonProperty("delivery_mode")
    private Integer deliveryMode;

    @JsonProperty("delivery_info")
    private Map<String, String> deliveryInfo;

    private Integer priority;

    @JsonProperty("body_encoding")
    private String bodyEncoding;

    @JsonProperty("delivery_tag")
    private String deliveryTag;

    public JedisMessageProperties() {
        this.bodyEncoding = "base64";
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    public Integer getDeliveryMode() {
        return deliveryMode;
    }

    public void setDeliveryMode(Integer deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public Map<String, String> getDeliveryInfo() {
        return deliveryInfo;
    }

    public void setDeliveryInfo(Map<String, String> deliveryInfo) {
        this.deliveryInfo = deliveryInfo;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getBodyEncoding() {
        return bodyEncoding;
    }

    public void setBodyEncoding(String bodyEncoding) {
        this.bodyEncoding = bodyEncoding;
    }

    public String getDeliveryTag() {
        return deliveryTag;
    }

    public void setDeliveryTag(String deliveryTag) {
        this.deliveryTag = deliveryTag;
    }
}