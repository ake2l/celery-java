package com.geneea.celery;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.geneea.celery.backends.redis.RedisBackend;
import com.geneea.celery.backends.redis.jedismessage.JedisTaskMessage;
import com.geneea.celery.spi.Backend;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Streams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class RedisWorker {

    private final ObjectMapper jsonMapper;
    private final Lock taskRunning = new ReentrantLock();
    private final Backend backend;
    private final Jedis jedis;

    private static final Logger LOG = Logger.getLogger(RedisWorker.class.getName());

    public RedisWorker(Jedis jedis, Backend backend) {
        this.jedis= jedis;
        this.backend = backend;
        this.jsonMapper = new ObjectMapper();
    }

    // get task message from Redis and handle task
    public void handleDelivery(String queue) throws IOException, DispatchException {
        while (true) {
            List<String> receivedMessage = jedis.blpop(0, queue);

            // get task message
            JedisTaskMessage taskMessage = jsonMapper.readValue(receivedMessage.get(1), JedisTaskMessage.class);
            String contentEncoding = taskMessage.getContentEncoding();
            String bodyEncoding = taskMessage.getProperties().getBodyEncoding();
            String body = taskMessage.getBody();

            if (bodyEncoding.equalsIgnoreCase("base64")) {
                byte[] dencodedBody = Base64.getDecoder().decode(body);
                body = new String(dencodedBody, contentEncoding);
            }

            // handle task
            String taskId = taskMessage.getHeaders().get("id").toString();

            taskRunning.lock();
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();

                JsonNode payload = jsonMapper.readTree(body);

                String taskClassName = taskMessage.getHeaders().get("task").toString();
                Object result = processTask(taskClassName, (ArrayNode) payload.get(0));

                LOG.info(String.format("CeleryTask %s[%s] succeeded in %s. Result was: %s",
                        taskClassName, taskId, stopwatch, result));

                backend.reportResult(taskId, taskMessage.getProperties().getReplyTo(),
                        taskMessage.getProperties().getCorrelationId(), result);

            } catch (DispatchException e) {
                LOG.log(Level.SEVERE, String.format("CeleryTask %s dispatch error", taskId), e.getCause());
                backend.reportException(taskId, taskMessage.getProperties().getReplyTo(),
                        taskMessage.getProperties().getCorrelationId(), e);
            } catch (JsonProcessingException e) {
                LOG.log(Level.SEVERE, String.format("CeleryTask %s - %s", taskId, e), e.getCause());
                backend.reportException(taskId, taskMessage.getProperties().getReplyTo(),
                        taskMessage.getProperties().getCorrelationId(), e);
            } catch (RuntimeException e) {
                LOG.log(Level.SEVERE, String.format("CeleryTask %s - %s", taskId, e), e);
                backend.reportException(taskId, taskMessage.getProperties().getReplyTo(),
                        taskMessage.getProperties().getCorrelationId(),
                        e.getCause() != null ? e.getCause() : e);
            } finally {
                taskRunning.unlock();
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Object processTask(String taskName, ArrayNode args) throws DispatchException {

        List<String> name = ImmutableList.copyOf(Splitter.on("#").split(taskName).iterator());

        if (name.size() != 2) {
            throw new DispatchException(MessageFormat.format(
                    "This worker can only process tasks with name in form package.ClassName#method, got {0}",
                    taskName));
        }

        Object task = TaskRegistry.getTask(name.get(0));

        if (task == null) {
            throw new DispatchException(String.format("CeleryTask %s not registered.", taskName));
        }

        Method method = Arrays.stream(task.getClass().getDeclaredMethods())
                .filter((m) -> m.getName().equals(name.get(1)))
                .collect(MoreCollectors.onlyElement());

        List<?> convertedArgs = Streams.mapWithIndex(
                Arrays.stream(method.getParameterTypes()),
                (paramType, i) -> jsonMapper.convertValue(args.get((int) i), paramType)
        ).collect(Collectors.toList());

        try {
            return method.invoke(task, convertedArgs.toArray());
        } catch (IllegalAccessException e) {
            throw new DispatchException(String.format("Error calling %s", method), e);
        } catch (InvocationTargetException e) {
            throw new AssertionError(String.format("Error calling %s", method), e);
        }
    }

    public void close() throws IOException {
        jedis.close();
        backend.close();
    }

    public void join() {
        taskRunning.lock();
        taskRunning.unlock();
    }

    public static RedisWorker create(String queue, Jedis jedis) throws IOException, DispatchException {
        RedisBackend redisBackend = new RedisBackend(jedis);
        final RedisWorker consumer = new RedisWorker(jedis, redisBackend);

        consumer.handleDelivery(queue);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                consumer.close();
                consumer.join();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        return consumer;
    }

    private static class Args {
        @Parameter(names = "--broker", description = "Broker URL, e. g. redis://localhost//")
        private String broker = "redis://localhost:6379/";
        @Parameter(names = "--queue", description = "Celery queue to watch")
        private String queue = "celery";
        @Parameter(names = "--concurrency", description = "Number of concurrent tasks to process")
        private int numWorkers = 2;
    }

    public static void main(String[] argv) throws Exception {
        Args args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(args.numWorkers); // maximum number of connections
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), args.broker);


        for (int i = 0; i < args.numWorkers; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        create(args.queue, jedisPool.getResource());
                    } catch (IOException | DispatchException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            thread.start();
        }

        System.out.println(String.format("Started consuming tasks from queue %s.", args.queue));
        System.out.println("Known tasks:");
        for (String taskName : TaskRegistry.getRegisteredTaskNames()) {
            System.out.print("  - ");
            System.out.println(taskName);
        }
    }
}
