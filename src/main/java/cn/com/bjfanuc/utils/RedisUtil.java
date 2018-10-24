package cn.com.bjfanuc.utils;

import cn.com.bjfanuc.App;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class RedisUtil {

    private String host = "127.0.0.1"; //默认配置
    private Integer port = 6379;//默认配置
    private Jedis jedis;//默认配置
    private String password = "";//默认配置
    private int retryDelay = 2000;//默认配置
    private Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    /**
     * 从App中获取Redis配置
     */ {

        host = App.redis != null ? App.redis.elementText("host") : host;
        password = App.redis != null ? App.redis.elementText("password") : password;
        try {

            port = Integer.parseInt(App.redis.elementText("port"));

        } catch (Exception e) {
            logger.info("Redis: port invalid，use default");
        }
        try {

        } catch (NumberFormatException e) {
            logger.info("Redis: retryTimes invalid，use default");

        }
        try {
            retryDelay = Integer.parseInt(App.taos == null ? null : App.taos.attributeValue("retryDelay"));
        } catch (NumberFormatException e) {
            logger.info("Redis: retryDelay invalid，use default");

        }
    }

    public RedisUtil() {
        connect();
        logger.info("Redis: connect to " + host + ":" + port);
    }

    /**
     * /连接Redis，连接失败会重连
     */
    public void connect() {
        int currentRetryTime = 1;
        while (true) {
            try {
                jedis = new Jedis(host, port);
                if (!password.equals(""))
                    jedis.auth(password);
                return;

            } catch (Exception e) {
                logger.error("Redis: connect failed: " + e.getMessage() + " retry " + (currentRetryTime++) + " time(s)" + " ");
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }

        }
    }

    public String get(String key) {
        String value = null;
        int currentRetryTime = 1;
        while (true) {
            try {
                value = jedis.get(key);
                break;
            } catch (Exception e) {
                System.err.println("Redis: connect failed: " + e.getMessage() + " retry " + (currentRetryTime++) + " time(s)" + " ");
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return value;
    }
}
