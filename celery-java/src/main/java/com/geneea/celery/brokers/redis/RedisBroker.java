package com.geneea.celery.brokers.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.geneea.celery.backends.redis.jedismessage.JedisTaskMessage;
import com.geneea.celery.spi.Broker;
import com.geneea.celery.spi.Message;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RedisBroker implements Broker {

    private final JedisPool jedisPool;
    public RedisBroker(JedisPool pool) {
        this.jedisPool = pool;
    }

    @Override
    public void declareQueue(String name) throws IOException {
        // do nothing
    }

    @Override
    public Message newMessage() {
        return new JedisMessage();
    }

    // prepare and send message to Redis server
    class JedisMessage implements Message {

        private byte[] body;
        private final JedisTaskMessage.Builder messageProps = new JedisTaskMessage.Builder()
                .deliveryMode(2)
                .priority(0);

        private final JedisHeaders headers = new JedisHeaders();

        @Override
        public void setBody(byte[] body) {
            this.body = body;
        }

        @Override
        public void setContentEncoding(String contentEncoding) {
            messageProps.contentEncoding(contentEncoding);
        }

        @Override
        public void setContentType(String contentType) {
            messageProps.contentType(contentType);
        }

        @Override
        public Headers getHeaders() {
            return headers;
        }

        // send message to celery worker
        @Override
        public void send(String queue) throws IOException {
            JedisTaskMessage jedisTaskMessage = messageProps
                    .headers(headers.map)
                    .body(body)
                    .build();

            // put message in JSON type
            ObjectMapper jsonMapper = new ObjectMapper();
            String taskMessage = jsonMapper.writeValueAsString(jedisTaskMessage);

            try (Jedis jedis = jedisPool.getResource()) {
                jedis.rpush(queue, taskMessage);
            } catch(JedisConnectionException ex) {
                ex.printStackTrace();
            }
        }

        // make message header
        class JedisHeaders implements Headers {

            private final Map<String, Object> map = new HashMap<>();

            JedisHeaders() {
                map.put("timelimit", Arrays.asList(null, null));
                map.put("retries", 0);
                map.put("parent_id", null);
                map.put("kwargsrepr", "{}");
                map.put("expires", null);
                map.put("eta", null);
                map.put("lang", "py"); // sic
                map.put("group", null);
            }

            @Override
            public void setId(String id) {
                messageProps.correlationId(id);
                map.put("root_id", id);
                map.put("id", id);
            }

            @Override
            public void setArgsRepr(String argsRepr) {
                map.put("argsrepr", argsRepr);
            }

            @Override
            public void setOrigin(String origin) {
                map.put("origin", origin);
            }

            @Override
            public void setReplyTo(String clientId) {
                messageProps.replyTo(clientId);
            }

            @Override
            public void setTaskName(String task) {
                map.put("task", task);
            }
        }
    }
}
