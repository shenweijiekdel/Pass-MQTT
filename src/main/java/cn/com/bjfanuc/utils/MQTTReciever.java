package cn.com.bjfanuc.utils;

import cn.com.bjfanuc.App;
import cn.com.bjfanuc.Count;
import com.alibaba.fastjson.JSONObject;
import org.dom4j.Element;
import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class MQTTReciever {
    private class MyTracer extends Tracer {
        public MyTracer() {

        }

        @Override
        public void debug(String message, Object... args) {
            logger.info(message);
            if (args.length > 0) {
                for (int i = 0; i < args.length; i++) {
                    logger.info(args[i].toString());
                }
            }
        }

        @Override
        public void onSend(MQTTFrame frame) {
        }

        @Override
        public void onReceive(MQTTFrame frame) {
        }
    }

    private String host = App.emq != null ? App.emq.elementText("host") : null;   //从App类获取emq配置信息
    private Integer port;
    private String user = App.emq != null ? App.emq.elementText("user") : null;   //从App类获取emq配置信息
    private String password = App.emq != null ? App.emq.elementText("password") : null;   //从App类获取emq配置信息
    private String encode = App.emq != null ? App.emq.elementText("encode") : null;   //从App类获取emq配置信息
    private List<Element> xmlTopics = App.emq != null ? App.emq.elements("topic") : null;   //从App类获取emq配置信息
    private MQTT mqtt = new MQTT();
    private List<Topic> topics = new ArrayList<>();
    private FutureConnection connection;
    private BlockingQueue<JSONObject> blockingQueue;
    private Logger logger = LoggerFactory.getLogger(MQTTReciever.class);

    public MQTTReciever(BlockingQueue<JSONObject> blockingQueue) {
        this.blockingQueue = blockingQueue;
        try {
            port = App.emq != null ? Integer.parseInt(App.emq.elementText("port")) : null;
        } catch (NumberFormatException e) {
            logger.warn("EMQ: port invalid，use default");
        }
        try {
            if (host == null) {
                logger.warn("EMQ: host invalid，use default");
                host = "localhost";
            }
            if (port == null) {
                logger.warn("EMQ: port invalid，use default");
                port = 1883;

            }
            if (encode == null) {
                logger.warn("EMQ: encoding invalid，use default");
                encode = "utf-8";

            }
            mqtt.setHost(host, port);

        } catch (URISyntaxException e) {
            logger.error("EMQ: host config incorrect,exit now!");
            System.exit(1);
        }
        mqtt.setClientId(UUID.randomUUID().toString());
        mqtt.setUserName(user != null ? user : "");
        mqtt.setPassword(password != null ? password : "");
        mqtt.setTrafficClass(8);
        mqtt.setSendBufferSize(64);
        mqtt.setReceiveBufferSize(1024);
        mqtt.setTracer(new MyTracer());
        connection = mqtt.futureConnection(); //使用future连接
        connection.connect();


        if (xmlTopics != null) {   //topic可以配置多个
            for (Element element : xmlTopics
            ) {
                String name = element.elementText("name");
                String qos = element.elementText("qos");
                try {

                    topics.add(new Topic(name != null ? name : "", qos != null ? QoS.values()[Integer.parseInt(qos)] : QoS.EXACTLY_ONCE));
                } catch (Exception e) {
                    logger.error(e.toString());
                }

            }
        }
        Topic[] topicArr = new Topic[topics.size()];
        topicArr = topics.toArray(topicArr);
        connection.subscribe(topicArr);
        logger.info("EMQ: user " + user + " connect to " + host + ":" + port);
    }

    /**
     * 接收消息并缓存到阻塞队列
     *
     * @throws Exception
     */
    public void recieve() throws Exception {
        Future<Message> futrueMessage = connection.receive();
        Message message = futrueMessage.await();
        String data = new String(message.getPayloadBuffer().toByteArray(), encode);
        JSONObject jsonObject = JSONObject.parseObject(data);
        Count.mqttRecieveDataNum++;

        if (blockingQueue.size() == App.bufferSize) //如果队列满了则旧数据出队，新数据再入队
            blockingQueue.take();
        blockingQueue.put(jsonObject);
        Count.dataInQueueNum++;
        message.ack(); //响应ACK，


    }
}
