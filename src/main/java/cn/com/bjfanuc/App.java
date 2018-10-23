package cn.com.bjfanuc;


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
 *
 */

public class App {
    public static  String  PATH = App.class.getProtectionDomain().getCodeSource().getLocation().getPath();
public static String HOME = System.getenv("HOME");
    public static Logger logger = LoggerFactory.getLogger(App.class);
    public static class DoStore implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    JSONObject take =  null;

                    take = blockingQueue.take();
                        Count.readDataFromQueueNum[Integer.parseInt(Thread.currentThread().getName())] ++;



                        dataService.save(take);

                }  catch (InterruptedException e) {
                   logger.error(e.getMessage());
                } finally {
                    System.out.println( Count.print());

                }
            }
        }
    }
    public static class DoRecieve implements Runnable{

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
                } catch (NullPointerException e){

                   logger.error("data error: NULL");

                }catch (Exception e) {
                    logger.error("other error: " + e);
                }


            }
        }
    }

    public static  Integer bufferSize = 1024;
    public static Element emq;
    public static Element taos;
    public static Element redis;
    public static List<String> taosHosts = new ArrayList<>();
    static {
        if (HOME == null){
            logger.info("please set environment variable HOME.");
            System.exit(1);

        }
        if (PATH == null){
            logger.info("cannot get current path,exit now");
            System.exit(1);
        }
        PATH = PATH.replaceAll(".jar",".xml");
       /* int i = PATH.lastIndexOf("/");
         PATH = PATH.substring(0,i+1) ;*/
        SAXReader reader = new SAXReader();
        try {
//            Document document = reader.read(new File("D:/settings.xml"));
            Document document = reader.read(new File(PATH));
          Element  xmlRoot = document.getRootElement();
         emq = xmlRoot.element("emq");
         taos = xmlRoot.element("taos-db");
         redis = xmlRoot.element("redis");
            List<Element> hostsE = App.taos != null?App.taos.elements("host"):null;
            if (hostsE != null || hostsE.size() > 0){
                for (Element e :hostsE
                ) {
                    taosHosts.add(e.getData().toString());

                }
            }
        } catch (DocumentException e){
//            e.printStackTrace();
            logger.error(e.getNestedException().toString());
        }
        try {
         bufferSize = Integer.parseInt(emq == null?null:emq.elementText("bufferSize"));

        }catch (NumberFormatException e){
            logger.warn("EMQ: bufferSize invalid，use default");
        }
    }
    public static  DataService dataService = SingletonFactory.getBean(DataServiceImpl.class.getName());
    public static BlockingQueue<JSONObject> blockingQueue = new ArrayBlockingQueue<>(bufferSize);
    public static  MQTTReciever mqttReciever;

    public static void main(String[] args) {
        try {

         mqttReciever = new MQTTReciever(blockingQueue);
        } catch (XMLReaderException e){
            logger.error(e.getMessage());
            System.exit(1);
        }
        int val = -1;
        try {
            if (( val = dataService.createDatabase()) != ReturnValue.SUCCESS){

                System.exit(1);
            }
        } catch (SQLException e) {
            logger.error("database create failed.exit now! :error "  +e.getErrorCode() );
            System.exit(1);
        }
        DoStore doStore = new DoStore();
        DoRecieve doRecieve = new DoRecieve();
        Thread recieveThread = new Thread(doRecieve);
        Thread storeThread[] = new Thread[taosHosts.size()>0?taosHosts.size():1];
        recieveThread.start();
        for (int i=0; i<storeThread.length; i++){
            storeThread[i] = new Thread(doStore,i + "");
            storeThread[i].start();
        }





    }
}