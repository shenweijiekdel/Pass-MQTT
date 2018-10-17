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
    private class MyTracer extends Tracer{
        public MyTracer() {

        }

        @Override
        public void debug(String message, Object... args) {
            logger.info(message);
           if (args.length > 0){
               for (int i=0; i<args.length; i++){
                   logger.info(args[i].toString());
               }
           }
        }

        @Override
        public void onSend(MQTTFrame frame) {
//        logger.info("onsend");
        }

        @Override
        public void onReceive(MQTTFrame frame) {
//        logger.info("onrecieve");
        }
    }
    private String host = App.emq != null?App.emq.elementText("host"):null;
    private Integer port;
    private String user = App.emq != null?App.emq.elementText("user"):null;
    private String password = App.emq != null?App.emq.elementText("password"):null;
    private String encode = App.emq != null?App.emq.elementText("encode"):null;
    private List<Element> xmlTopics = App.emq != null?App.emq.elements("topic"):null;
    private MQTT mqtt = new MQTT();
    private List<Topic> topics = new ArrayList<>();
    private FutureConnection connection;
    private BlockingQueue<JSONObject> blockingQueue;
    private Logger logger = LoggerFactory.getLogger(MQTTReciever.class);
    public MQTTReciever( BlockingQueue<JSONObject> blockingQueue) {
        this.blockingQueue = blockingQueue;
        try {
            port = App.emq != null?Integer.parseInt(App.emq.elementText("port")):null;
        }catch (NumberFormatException e){
            logger.warn("EMQ: port invalid，use default");
        }
        try {
            if (host == null){
                logger.warn("EMQ: host invalid，use default");
                host = "localhost";
            }
            if (port == null){
                logger.warn("EMQ: port invalid，use default");
                port = 1883;

            }
            if (encode == null){
                logger.warn("EMQ: encoding invalid，use default");
                encode = "utf-8";

            }
        mqtt.setHost(host, port );

        }  catch (URISyntaxException e) {
            logger.error("EMQ: host config incorrect,exit now!");
            System.exit(1);
        }
        mqtt.setClientId(UUID.randomUUID().toString());
        mqtt.setUserName(user != null?user:"");
        mqtt.setPassword(password != null?password:"");
        mqtt.setTrafficClass(8);
        mqtt.setTracer(new MyTracer());
        connection = mqtt.futureConnection();
        connection.connect();

        if (xmlTopics != null){
            for (Element element:xmlTopics
                 ) {
                String name = element.elementText("name");
                String qos = element.elementText("qos");
                try {

                topics.add(new Topic(name != null?name:"",qos != null? QoS.values()[Integer.parseInt(qos)]:QoS.EXACTLY_ONCE));
                } catch (Exception e){
                    logger.error(e.toString());
                }

            }
        }
        Topic[] topicArr = new Topic[topics.size()];
        topicArr = topics.toArray(topicArr);
        connection.subscribe(topicArr);
            logger.info("EMQ: user "+user+" connect to " + host + ":" + port);
    }
    public void recieve() throws Exception {
        Future<Message> futrueMessage = connection.receive();
        Message message = futrueMessage.await();
        String data = new String(message.getPayloadBuffer().toByteArray(), encode);
        JSONObject jsonObject = JSONObject.parseObject(data);
        Count.incMqttRecieveDataNum();
        if (blockingQueue.size() == App.bufferSize)
            blockingQueue.remove();
        blockingQueue.put(jsonObject);
        Count.incDataInQueueNum();
        message.ack();


    }
}
