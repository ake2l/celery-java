package com.geneea.celery.backends.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.geneea.celery.WorkerException;
import com.geneea.celery.backends.TaskResult;
import com.geneea.celery.spi.Backend;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedisResultConsumer implements  Backend.ResultsProvider{

    private final LoadingCache<String, SettableFuture<Object>> tasks =
            CacheBuilder
                    .newBuilder()
                    .expireAfterWrite(2, TimeUnit.HOURS)
                    .build(new CacheLoader<String, SettableFuture<Object>>() {
                        @Override
                        public SettableFuture<Object> load(@Nonnull String s) throws Exception {
                            return SettableFuture.create();
                        }
                    });

    private final Jedis jedis;

    // key to receive value from Redis
    private String key;

    public RedisResultConsumer(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public ListenableFuture<Object> getResult(String taskId) {

        try {

            // use Jedis psubscribe to wait for the result message from celery worker
            Runnable runnable1 = () -> {
                JedisPubSub jedisPubSub = new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        // unsubscribe after received result message
                        key = channel;
                        unsubscribe(channel);
                    }

                    @Override
                    public void onPMessage(String pattern, String channel, String message) {
                        // unsubscribe after received result message
                        key = channel;
                        punsubscribe(pattern);
                    }
                };

                jedis.psubscribe(jedisPubSub, "*" + taskId + "*");
            };

            Thread myThread = new Thread(runnable1);
            myThread.start();
            myThread.join();

            // get result and delete it from Redis after thread end
            String result = jedis.get(key);
            jedis.del(key);

            // handle the result
            handleDelivery(result);

        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return tasks.getUnchecked(taskId);
    }

    public void handleDelivery (String result) throws JsonProcessingException {

        TaskResult payload = new ObjectMapper().readValue(result, TaskResult.class);

        SettableFuture<Object> future = tasks.getUnchecked(payload.taskId);

        boolean setAccepted;

        // check result status, answer to client
        if (payload.status == TaskResult.Status.SUCCESS) {
            setAccepted = future.set(payload.result);
        } else {
            @SuppressWarnings("unchecked")
            Map<String, String> exc = (Map<String, String>) payload.result;
            setAccepted = future.setException(
                    new WorkerException(exc.get("exc_type"), exc.get("exc_message")));
        }
        assert setAccepted;
    }
}
