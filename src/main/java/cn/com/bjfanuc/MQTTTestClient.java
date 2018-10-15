package cn.com.bjfanuc;

import com.alibaba.fastjson.JSONObject;
import org.fusesource.mqtt.client.*;

import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

class TestMQTT implements Runnable {
    public int count = 0;
    public static JSONObject jsonObject = JSONObject.parseObject("{\n" +
            "    \"MSGTYPE\": \"REP\",\n" +
            "    \"COMMAND\": \"ADD_INFO\",\n" +
            "    \"SUBCMD\": \"FANUC_CNC\",\n" +
            "    \"DATA\": {\n" +
            "        \"CNC_ID\": \"FC_00E0E416DD8E\",\n" +
            "        \"CNC_IP\": \"192.168.1.111\",\n" +
            "        \"DTU_ID\": \"FB-A001-A2FBA20\",\n" +
            "        \"DTU_SIM_ICCID\": \"89860617020020705327\",\n" +
            "        \"DTU_4G_IP\": \"122.97.176.146\",\n" +
            "        \"CNC_DAQTIME\": \"2017-10-13 13:12:50\",\n" +
            "        \"CNC_CONN\": 0\n" +
            "    }\n" +
            "}\n");
    private MQTTTestClient client = new MQTTTestClient();
    private JSONObject data;
    public TestMQTT() {

    }

    @Override
    public void run() {
        while (true) {

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Ms");
            String format1 = format.format(new Date().getTime());
            System.out.println(format1);
            jsonObject.getJSONObject("DATA").put("CNC_DAQTIME",new Date().getTime());
            jsonObject.getJSONObject("DATA").put("CNC_ID",new Date());
            jsonObject.getJSONObject("DATA").put("CNC_ID",Thread.currentThread().getName());
            client.publish(jsonObject.toJSONString());
            System.out.println( "线程" + Thread.currentThread().getName() + ": 第 " + count + "条数据");
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}

public class MQTTTestClient {
    private MQTT mqtt = new MQTT();
    private FutureConnection futureConnection;
    private  int i = 0;
    public MQTTTestClient() {
        try {
            mqtt.setHost("192.168.12.49", 1883);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        mqtt.setClientId(UUID.randomUUID().toString());
        mqtt.setUserName("CNCUser");
        mqtt.setPassword("user");
        mqtt.setTracer(new Tracer());
        futureConnection = mqtt.futureConnection();
        futureConnection.connect();
    }

    public void publish(String msg) {
        if (i == 10)
            Thread.currentThread().stop();
        futureConnection.publish("dev/15a09834/up/1/FANUC_CNC/1", msg.getBytes(), QoS.values()[0], false);
        i ++;
    }

    public void disconnect() {
        futureConnection.disconnect();
    }

    public static void main(String[] args) {
       Thread[] threads = new Thread[1];
        for (int i=0; i<threads.length; i++){
            threads[i] = new Thread(new TestMQTT(),"FC_Thread_" + i);
        }
       for (Thread thread:threads){
           thread.start();
       }


    }

}







