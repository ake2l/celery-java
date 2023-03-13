package com.geneea.celery.backends.redis;

import com.geneea.celery.spi.Backend;
import com.geneea.celery.spi.BackendFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.http.client.utils.URIBuilder;
import org.kohsuke.MetaInfServices;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

@MetaInfServices(BackendFactory.class)
public class RedisBackendFactory implements BackendFactory {
    @Override
    public Set<String> getProtocols() {
        return ImmutableSet.of("redis");
    }

    @Override
    public Backend createBackend(URI uri, ExecutorService executor) throws IOException, TimeoutException {

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20); // maximum number of connections
        jedisPoolConfig.setMinIdle(5); // minimum number of idle connections

        JedisPool pool = new JedisPool(jedisPoolConfig, uri);

        return new RedisBackend(pool.getResource());
    }
}
