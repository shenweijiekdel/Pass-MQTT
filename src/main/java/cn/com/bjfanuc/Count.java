package cn.com.bjfanuc;

public class Count {
    public static volatile int createTableNum = 0;
    public static volatile int mqttRecieveDataNum = 0;
    public static volatile int readDataFromQueueNum = 0;
    public static volatile int dataInQueueNum = 0;
    public static volatile int savedDataNum = 0;
    public static volatile int saveSuccess = 0;
    public static volatile int saveFailure = 0;
    public static synchronized  void incCreateTableNum(){
        createTableNum ++;
    }
    public static synchronized void incMqttRecieveDataNum(){
        mqttRecieveDataNum ++;
    }
    public static synchronized void incReadDataFromQueueNum(){
        readDataFromQueueNum ++;
    }
    public static synchronized void incDataInQueueNum(){
        dataInQueueNum ++;
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
    }
    public static String  print(){
//        String s = "建表数：" + createTableNum + ",\t接受数：" + mqttRecieveDataNum +",\t入队数：" + dataInQueueNum +  ",\t出队数" + readDataFromQueueNum +  ",\t执行存储数：" +savedDataNum + ",\t成功数：" + saveSuccess + ",\t失败数：" + saveFailure;
String s = "入队数：" + dataInQueueNum + "存储数：" + saveSuccess;
        return "线程" +Thread.currentThread().getName() + ": " + s;
    }

}
