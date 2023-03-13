package com.geneea.celery.brokers.redis;

import com.geneea.celery.spi.Backend;
import com.geneea.celery.spi.Broker;
import com.geneea.celery.spi.BrokerFactory;
import com.google.common.collect.ImmutableSet;
import org.kohsuke.MetaInfServices;
import redis.clients.jedis.*;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@MetaInfServices(BrokerFactory.class)
public class RedisBrokerFactory implements BrokerFactory {
    @Override
    public Set<String> getProtocols() {
        return ImmutableSet.of("redis");
    }

    @Override
    public Broker createBroker(URI uri, ExecutorService executor) {

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20); // maximum number of connections
        jedisPoolConfig.setMinIdle(5); // minimum number of idle connections

        JedisPool pool = new JedisPool(jedisPoolConfig, uri);

        return new RedisBroker(pool);
    }

}
