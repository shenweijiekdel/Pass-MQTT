package cn.com.bjfanuc;

import java.util.List;

public class Count {
    public static  int threadNum = App.taosHosts.size();
    public static  int[] createTableNum = new int[threadNum];
    public static  volatile  int mqttRecieveDataNum = 0;
    public static  int[] readDataFromQueueNum = new int[threadNum];
    public static  volatile  int dataInQueueNum = 0;
    public static  int[] saveSuccess = new int[threadNum];
   /* public static synchronized void incMqttRecieveDataNum(){
        mqttRecieveDataNum ++;
    }
    public static synchronized void incDataInQueueNum(){
        dataInQueueNum ++;
    }*/
  /*  public static synchronized  void incCreateTableNum(){
        createTableNum ++;
    }
    public static synchronized void incReadDataFromQueueNum(){
        readDataFromQueueNum ++;
    }
    public static synchronized void incSavedDataNum(){
        savedDataNum ++;
    }
    public static synchronized void incSaveSuccess(){
        saveSuccess ++;
    }
    public static synchronized void incSaveFailure(){
        saveFailure ++;
    }
    private Count(){}
    public static void clearTableNum(){
        createTableNum = 0;
    }
    public static void clearRecieveDataNum(){
        mqttRecieveDataNum = 0;
    }
    public static void clearSaveDataNum(){
        savedDataNum = 0;
    }*/
    public static String  print(){
        String s = "建表数：" + createTableNum[Integer.parseInt(Thread.currentThread().getName())] + ",\t接收数：" + mqttRecieveDataNum +",\t入队数：" + dataInQueueNum +  ",\t出队数" + readDataFromQueueNum[Integer.parseInt(Thread.currentThread().getName())] + "\t存储数：" + saveSuccess[Integer.parseInt(Thread.currentThread().getName())];
//String s = "入队数：" + dataInQueueNum + " 存储数：" + saveSuccess;
        return "线程" +Thread.currentThread().getName() + ": " + s;
    }

}
