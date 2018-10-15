package cn.com.bjfanuc;


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
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;


/**
 * Hello world!
 * 18:06:11[com.taosdata.jdbc.TSDBConnectionImpl(106)-ERROR] failed to connect to server
 * java.sql.SQLException: TDengine Error:failed to connect to server
 * 	at com.taosdata.jdbc.TSDBConnectionImpl.createStatement(TSDBConnectionImpl.java:107)
 * 	at TaosUtil.createStatementWithReconnect(TaosUtil.java:89)
 * 	at TaosUtil.saveDataBeforeTaosNextVersion(TaosUtil.java:169)
 * 	at DataDaoImpl.saveData(DataDaoImpl.java:43)
 * 	at DataServiceImpl.save(DataServiceImpl.java:54)
 * 	at App$DoStore.run(App.java:47)
 * 	at java.lang.Thread.run(Thread.java:748)
 * 存在问题在初始化时没连上数据库的话，之后连接也一直失败
 */

public class App {

    public static class DoStore implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    JSONObject take =  null;

                    take = blockingQueue.take();
                    Count.incReadDataFromQueueNum();


                    int   save = 0;

                        save = dataService.save(take);

                    Count.incSavedDataNum();
                    if (save == 1){
//                        while (save -- > 0){
                            Count.incSaveSuccess();
//                        }
                    } else{
                        Count.incSaveFailure();

                    System.out.println( Count.print() + " return：" + save);
                    }
                    if (Count.saveSuccess % 10000== 0)
                        System.out.println( Count.print());
                }  catch (InterruptedException e) {
                    e.printStackTrace();
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
                    System.err.println("data error: " + e.getMessage());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnsupportedEncodingException e) {
                    System.err.println("encode error: " + e.getMessage());
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
        }
    }

    public static final int BUFFER_SIZE = 1024;
    public static Element emq;
    public static Element taos;
    public static Element redis;
    public static List<String> taosHosts = new ArrayList<>();
    public static  String  PATH ;
    static {
        System.out.println(App.class.getClassLoader().getResourceAsStream(""));  //待完善
        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(new File("settings.xml"));
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
        } catch (DocumentException e) {
            System.out.println(e.getMessage());
        }
    }
    public static  DataService dataService = SingletonFactory.getBean(DataServiceImpl.class.getName());
    public static BlockingQueue<JSONObject> blockingQueue = new ArrayBlockingQueue<>(BUFFER_SIZE);
    public static String configFilePath = System.getProperty("PASS_CONFIG");
    public static  MQTTReciever mqttReciever;

    public static void main(String[] args) throws MqttException {
          /*  if (PATH == null){
                System.err.println("path get failed.exit now!");
            System.exit(1);
            }
*/
        try {

         mqttReciever = new MQTTReciever(blockingQueue);
        } catch (XMLReaderException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }
        if (dataService.createDatabase() != 1){
            System.err.println("database create failed.exit now!");
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