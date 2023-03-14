package com.geneea.celery.backends.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geneea.celery.backends.TaskResult;
import com.geneea.celery.spi.Backend;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class RedisBackend implements Backend {

    private final Jedis jedis;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    public RedisBackend(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public ResultsProvider resultsProviderFor(String clientId) {
        RedisResultConsumer consumer = new RedisResultConsumer(jedis);
        return consumer;
    }

    @Override
    public void reportResult(String taskId, String queue, String correlationId, Object result) throws JsonProcessingException {

        TaskResult res = new TaskResult();
        res.result = result;
        res.taskId = taskId;
        res.status = TaskResult.Status.SUCCESS;

        String resultMessage = jsonMapper.writeValueAsString(res);
        // send result
        jedis.publish("celery-task-meta-" + taskId , resultMessage);
        // storage result
        jedis.set("celery-task-meta-" + taskId , resultMessage);
    }

    @Override
    public void reportException(String taskId, String queue, String correlationId, Throwable e) throws JsonProcessingException {
        Map<String, String> excInfo = new HashMap<>();
        excInfo.put("exc_type", e.getClass().getSimpleName());
        excInfo.put("exc_message", e.getMessage());

        TaskResult res = new TaskResult();
        res.result = excInfo;
        res.taskId = taskId;
        res.status = TaskResult.Status.FAILURE;

        String resultMessage = jsonMapper.writeValueAsString(res);

        jedis.publish("celery-task-meta-" + taskId , resultMessage);
    }

    @Override
    public void close() {
        jedis.close();
    }
}
