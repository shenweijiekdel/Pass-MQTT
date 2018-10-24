package cn.com.bjfanuc;

import java.util.List;

public class Count {
    public static int threadNum = App.taosHosts.size();
    public static int[] createTableNum = new int[threadNum];
    public static volatile int mqttRecieveDataNum = 0;
    public static int[] readDataFromQueueNum = new int[threadNum];
    public static volatile int dataInQueueNum = 0;
    public static int[] saveSuccess = new int[threadNum];

    public static String print() {
        int threadIndex = Thread.currentThread().getName().charAt(0) - 48;
        String s = "建表数：" + createTableNum[threadIndex] + ",\t接收数：" + mqttRecieveDataNum + ",\t入队数：" + dataInQueueNum + ",\t出队数" + readDataFromQueueNum[threadIndex] + "\t存储数：" + saveSuccess[threadIndex];
        return "线程" + Thread.currentThread().getName() + ": " + s;
    }

}
