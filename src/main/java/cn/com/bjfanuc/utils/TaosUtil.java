package cn.com.bjfanuc.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import cn.com.bjfanuc.App;
import com.taosdata.jdbc.TSDBJNIConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

public class TaosUtil {
    private String host;
    private String user = App.taos != null ? App.taos.elementText("root") : null;
    private String password = App.taos != null ? App.taos.elementText("password") : null;
    private String port = "0";
    private String configDir = App.taos != null ? App.taos.elementText("config-dir") : null;
    private static final String TSDB_DRIVER = App.taos != null ? App.taos.elementText("driver-class") : null;
    private String jdbcUrl = null;
    private static final String JDBC_PROTOCAL = "jdbc:TSDB://";
    private Connection conn = null;
    private Statement statement = null;
    private int retryDelay = 2000; //动作执行失败后重试时间间隔
    private Logger logger = LoggerFactory.getLogger(TaosUtil.class);

    {

        try {
            if (TSDB_DRIVER == null){
                logger.error("TaosDB: JDBC Driver is null. exit now!");
                System.exit(1);
            }
            retryDelay = Integer.parseInt(App.taos == null?null:App.taos.attributeValue("retryDelay"));  //xml文件读取有问题
        }catch (NumberFormatException e){
            logger.warn("TaosDB: retryDelay invalid，use default");

        }
    }
    public TaosUtil(String host) {
        this.host = host;
        this.jdbcUrl = JDBC_PROTOCAL + (host == null ? "127.0.0.1" : host) + ":" + port + "/"
                + "?user=" + (user == null ? "root" : user) + "&password="
                + (password == null ? "taosdata" : password);
        TSDBJNIConnector.init(this.configDir);
        this.doConnectToTaosd();
        createStatementWithReconnect();
    }
    private Connection doConnectToTaosd() {
        try {

            Class.forName(TSDB_DRIVER);
            while (true){
               int currentRetryTime = 1;
            try {
                if (conn == null || conn.isClosed()) {
                    conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
                } else {

                    break;
                }
            } catch (Exception e) {

                logger.error("TaosDB: connect failed: "+e.getMessage()+" retry " + (currentRetryTime ++) + " time(s)" + ":" + e.getMessage());
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            }

        } catch (ClassNotFoundException e){
            logger.error("TaosDB: cannot find JDBC Driver!");
            System.exit(1);
        }
        return this.conn;
    }

    private void createStatementWithReconnect(){
        int currentRetryTime = 1;
        while (statement == null) {

            try {
                statement = conn.createStatement();
            } catch (SQLException e) {
                logger.error("TaosDB: connect failed!: "+e.getMessage()+" retry " + (currentRetryTime ++) + " time(s)");

                    reConnect();

                try {
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException e2) {
                    logger.error(e2.toString());
                    }


            } catch (NullPointerException e){

                    reConnect();

            }

        }
    }
    public int executeUpdateWithReconnect(String sql){
        int val = -1;
        int currentRetyTime = 1;
            while (statement == null){
              logger.error("TaosDB: statement create failed.retry " + currentRetyTime++ + " time(s)");
               createStatementWithReconnect();
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e) {
                   logger.error(e.toString());
                }
            }
            while (true) {

                try {
                    val= 1;
                   /* if (val != 1)
                        throw new SQLException();*/
                    val = statement.executeUpdate(sql.toString());
                } catch (SQLException e) {
                   logger.error("TaosDB: return: " + val + " errorCode:" + e);
                }

                if (val != 29)
                    break;
               logger.error("TaosDB: execute failed，retry " + (currentRetyTime ++) + " time(s)");
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e) {
                   logger.error(e.toString());

                }
            }
        return val;
    }
    /*public int saveDataBeforeTaosNextVersion(JSONObject value, String createSql) throws DataErrException {   //必须按建表时的字段顺序插入
        JSONObject jsonData = value.getJSONObject("DATA");
        if (jsonData == null) {
            throw new DataErrException("data format error!");
        }

        createSql = createSql.replaceAll("\\(", "").replaceAll("\\)", "");
        String[] s = createSql.split(",");
        String tableName = jsonData.getString("CNC_ID");
        StringBuffer sql = new StringBuffer("import into " + tableName);
        sql.append(" values (");
        for (int i = 0; i < s.length; i++) {
            s[i] = s[i].replaceAll("^\\s+", "");
            if (i == 0)
                sql.append("'").append(jsonData.get(s[i].split(" ")[0].trim().toUpperCase())).append("'");
            else
                sql.append(",").append("'").append(jsonData.get(s[i].split(" ")[0].trim().toUpperCase())).append("'");
        }
        sql.append(")");
        Statement statementWithReconnect = createStatementWithReconnect();



        return  executeUpdateWithReconnect(statementWithReconnect,sql.toString());
//            System.out.println("线程" + Thread.currentThread().getName() + ": " + sql);


    }*/



    public void reConnect() {
        doCloseConnection();

        doConnectToTaosd();


    }

    public void doCloseConnection() {
        try {
            if (this.conn != null)
                this.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            conn = null;
        }
    }
}
