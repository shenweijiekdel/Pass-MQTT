package cn.com.bjfanuc.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import cn.com.bjfanuc.App;
import cn.com.bjfanuc.Count;
import com.taosdata.jdbc.TSDBJNIConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

public class TaosUtil {

    private String host;  //Host由构造传递
    private String user = App.taos != null ? App.taos.elementText("root") : null;  //从App类获取taos配置信息
    private String password = App.taos != null ? App.taos.elementText("password") : null;//从App类获取taos配置信息
    private String port = "0";
    private String configDir = App.taos != null ? App.taos.elementText("config-dir") : null;//从App类获取taos配置信息
    private String jdbcUrl = null;
    private static final String TSDB_DRIVER = App.taos != null ? App.taos.elementText("driver-class") : null;//从App类获取taos配置信息
    private static final String JDBC_PROTOCAL = "jdbc:TSDB://";
    private Connection conn = null;
    private Statement statement = null;
    private int retryDelay = 2000;
    private Logger logger = LoggerFactory.getLogger(TaosUtil.class);

    {

        try {
            if (TSDB_DRIVER == null) {
                logger.error("TaosDB: JDBC Driver is null. exit now!");
                System.exit(1);
            }
            retryDelay = Integer.parseInt(App.taos == null ? null : App.taos.attributeValue("retryDelay"));  //重连延时会抛异常，单独处理
        } catch (NumberFormatException e) {
            logger.warn("TaosDB: retryDelay invalid，use default");

        }
    }

    public TaosUtil(String host) {
        this.host = host;
        this.jdbcUrl = JDBC_PROTOCAL + (host == null ? "127.0.0.1" : host) + ":" + port + "/"
                + "?user=" + (user == null ? "root" : user) + "&password="
                + (password == null ? "taosdata" : password);
        TSDBJNIConnector.init(this.configDir, "en_US.UTF-8", "Asia/Shanghai");
        this.doConnectToTaosd();
        createStatementWithReconnect();
    }

    /**
     * 连接taos，若失败则重连
     *
     * @return
     */
    private Connection doConnectToTaosd() {
        try {

            Class.forName(TSDB_DRIVER);
            int currentRetryTime = 1;
            while (true) {
                try {
                    if (conn == null || conn.isClosed()) {
                        conn = (Connection) DriverManager.getConnection(this.jdbcUrl);
                    } else {

                        break;
                    }
                } catch (Exception e) {

                    logger.error("TaosDB: connect failed, retry " + (currentRetryTime++) + " time(s)");
                    try {
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException e1) {
                        logger.error("other error: ", e1);
                    }
                }
            }

        } catch (ClassNotFoundException e) {
            logger.error("TaosDB: cannot find JDBC Driver!");
            System.exit(1);
        }
        return this.conn;
    }

    /**
     * 创建statement，若为空则重连
     */
    private void createStatementWithReconnect() {
        int currentRetryTime = 1;
        while (statement == null) {

            try {
                statement = conn.createStatement();
            } catch (SQLException e) {
                logger.error("TaosDB: connect failed!: " + e.getMessage() + " retry " + (currentRetryTime++) + " time(s)");

                reConnect();

                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e2) {
                    logger.error("other error: ", e2);
                }


            } catch (NullPointerException e) {

                reConnect();

            }

        }
    }

    /**
     * 执行SQL语句，若无法连接到服务则重试(JDBC驱动会在执行executeUpdata时自动重连服务)
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public int executeUpdateWithReconnect(String sql) throws SQLException {
        int val = -1;
        int currentRetyTime = 1;
        while (statement == null) {
            logger.error("TaosDB: statement create failed.retry " + currentRetyTime++ + " time(s)");
            createStatementWithReconnect();
            try {
                Thread.sleep(retryDelay);
            } catch (InterruptedException e) {
                logger.error("other error: ", e);
            }
        }
        while (true) {

            try {
                val = statement.executeUpdate(sql.toString());
                break;
            } catch (SQLException e) {
                if (e.getErrorCode() == ReturnValue.LOST_CONNECTION) {
                    try {
                        logger.error("TaosDB: execute failed，retry " + (currentRetyTime++) + " time(s)");
                        Thread.sleep(retryDelay);
                    } catch (InterruptedException e1) {
                        logger.error("other error: ", e1);
                    }
                } else {
                    throw e;
                }
            }


        }
        return val;
    }

    /**
     * 重连
     */
    public void reConnect() {
        doCloseConnection();

        doConnectToTaosd();


    }

    /**
     * 关闭连接
     */
    public void doCloseConnection() {
        try {
            if (this.conn != null)
                this.conn.close();
        } catch (SQLException e) {
            logger.error("close failed: ", e);
            conn = null;
        }
    }
}
