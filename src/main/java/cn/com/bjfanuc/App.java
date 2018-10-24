package cn.com.bjfanuc;


import cn.com.bjfanuc.exception.DataErrException;
import cn.com.bjfanuc.utils.ReturnValue;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import cn.com.bjfanuc.service.DataService;
import cn.com.bjfanuc.service.impl.DataServiceImpl;
import cn.com.bjfanuc.utils.MQTTReciever;
import cn.com.bjfanuc.utils.SingletonFactory;
import com.sun.xml.internal.ws.streaming.XMLReaderException;


import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;


/**
 * 主类
 */

public class App {
    public static String PATH = App.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    public static String HOME = System.getenv("HOME");
    public static Logger logger = LoggerFactory.getLogger(App.class);
    public static Integer bufferSize = 64;
    public static Element emq;
    public static Element taos;
    public static Element redis;
    public static List<String> taosHosts = new ArrayList<>();
    public static DataService dataService ;
    public static BlockingQueue<JSONObject> blockingQueue;  //建立缓冲区
    public static MQTTReciever mqttReciever;

    /**
     * 存储线程Runnable
     */
    public static class DoStore implements Runnable {

        @Override
        public void run() {

            JSONObject take = null;
                StringBuffer log = new StringBuffer();
            while (true) {
                log.delete(0,log.length());

                try {

                    take = blockingQueue.take();
                    Count.readDataFromQueueNum[0]++;


                    int val = dataService.save(take);
                    log.append("save data return ").append(val);
                    logger.info(log.toString());
                } catch (DataErrException e) {
                    logger.error("Data error: " + e.getMessage() + "\n.json: " + take);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                } catch (Exception e) {
                    logger.error("other save error: ", e);
                }finally {
                    System.out.println(Count.print());

                }
            }
        }
    }

    /**
     * 接收线程Runnable
     */
    public static class DoRecieve implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    mqttReciever.recieve();


                } catch (JSONException e) {
                    logger.error("data error: " + e.getMessage());
                } catch (InterruptedException e) {
                    logger.error(e.toString());
                } catch (UnsupportedEncodingException e) {
                    logger.error("encode error: " + e.getMessage());
                } catch (NullPointerException e) {

                    logger.error("data error: NULL");

                } catch (Exception e) {
                    logger.error("other error: " + e);
                }


            }
        }
    }


    static {
        /**
         * 必须设置HOME变量
         */
        if (HOME == null) {
            logger.error("please set environment variable HOME.");
            System.exit(1);

        }
        /**
         * 若PATH为空，则表示未获取到本地路径
         */
        if (PATH == null) {
            logger.error("cannot get current path,exit now");
            System.exit(1);
        }
        PATH = PATH.replaceAll(".jar", ".xml");

        /**
         * XML配置文件解析
         */
        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(new File(PATH));
            Element xmlRoot = document.getRootElement();
            emq = xmlRoot.element("emq");          //读取emq配置子节点
            taos = xmlRoot.element("taos-db");   //读取taos配置子节点
            redis = xmlRoot.element("redis");   //赌气redis配置子节点
            /**
             * taos配置中的Host可以是多个，根据host的数量决定线程启用数量
             */
            List<Element> hostsE = App.taos != null ? App.taos.elements("host") : null; //单独读取taos的host标签
            if (hostsE != null || hostsE.size() > 0) {
                for (Element e : hostsE
                ) {
                    taosHosts.add(e.getData().toString());

                }
            }
        } catch (DocumentException e) {
            logger.error(e.getNestedException().toString());
        }
        try {
            bufferSize = Integer.parseInt(emq == null ? null : emq.elementText("bufferSize"));  //读取缓冲区长度配置

        } catch (NumberFormatException e) {
            logger.warn("EMQ: bufferSize invalid，use default");
        }
       dataService = SingletonFactory.getBean(DataServiceImpl.class.getName());
        blockingQueue = new ArrayBlockingQueue<>(bufferSize * 1024);
        if (taosHosts.size() > 9){
            logger.error("maxmum of taos host is 9,exit now");
            System.exit(1);

        }
    }


    public static void main(String[] args) {
        /**
         * 创界MQTT接收器进行消息接收并缓存
         */
        try {

            mqttReciever = new MQTTReciever(blockingQueue);  //创建对象进行消息接收
        } catch (XMLReaderException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }

        /**
         * 创建数据库
         */
        int val = -1;
        try {
            if ((val = dataService.createDatabase()) != ReturnValue.SUCCESS) {

                System.exit(1);
            }
        } catch (SQLException e) {
            logger.error("database create failed.exit now! :error " + e.getErrorCode());
            System.exit(1);
        }
        /**
         * 创建接收线程
         */
        DoRecieve doRecieve = new DoRecieve();
        Thread recieveThread = new Thread(doRecieve);
        /**
         * 创建存储线程，线程数由taosHost数目决定，线程名为数字，方便线程隔离
         */
        DoStore doStore = new DoStore();
        Thread storeThread[] = new Thread[taosHosts.size() > 0 ? taosHosts.size() : 1];
        recieveThread.start();
        for (int i = 0; i < storeThread.length; i++) {
            storeThread[i] = new Thread(doStore, i + "");
            storeThread[i].start();
        }


    }
}