package com.geneea.celery.examples;

import com.geneea.celery.Celery;
import com.geneea.celery.DispatchException;
import com.geneea.celery.RedisWorker;
import com.google.common.base.Stopwatch;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;

public class RedisTestTask {

    public static void main(String[] args) throws Exception {

        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                RedisWorker worker = null;
                try {
                    worker = RedisWorker.create("celery", pool.getResource());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (DispatchException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (worker != null){
                        try {
                            worker.close();
                            worker.join();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        });
        thread.start();


        Celery client = Celery.builder()
                .brokerUri("redis://localhost:6379/")
                .backendUri("redis://localhost:6379/")
                .build();

        try {
            for (int i = 0; i < 20; i++) {
                Stopwatch sw = Stopwatch.createStarted();
                Integer result = TestTaskProxy.with(client).sum(1, i).get();
                System.out.printf("CeleryTask #%d's result was: %s. The task took %s end-to-end.\n", i, result, sw);
            }

            System.out.println("Testing result of void task: " + TestVoidTaskProxy.with(client).run(1, 2).get());
            System.out.println("Testing task that should fail and throw exception:");
            client.submit(TestTask.class, "sum", new Object[]{"a", "b"}).get();
        } finally {
            System.out.println("------end------");
            pool.close();
        }
    }
}
