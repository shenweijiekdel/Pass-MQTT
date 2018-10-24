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
        String s = "建表数：" + createTableNum[Integer.parseInt(Thread.currentThread().getName())] + ",\t接收数：" + mqttRecieveDataNum + ",\t入队数：" + dataInQueueNum + ",\t出队数" + readDataFromQueueNum[Integer.parseInt(Thread.currentThread().getName())] + "\t存储数：" + saveSuccess[Integer.parseInt(Thread.currentThread().getName())];
        return "线程" + Thread.currentThread().getName() + ": " + s;
    }

}
