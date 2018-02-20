package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;

    private final String LAST_TEN_SEARCHES_KEY = "last-10-searches";
    private final int BUFFER_SIZE = 10;

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public List<String> getLastTenSearches() {
        return jedis.lrange(LAST_TEN_SEARCHES_KEY, 0, BUFFER_SIZE - 1);
    }

    public void addLastSearch(String param) {
        jedis.lpush(LAST_TEN_SEARCHES_KEY, param);
        jedis.ltrim(LAST_TEN_SEARCHES_KEY, 0, BUFFER_SIZE - 1);
    }
}
