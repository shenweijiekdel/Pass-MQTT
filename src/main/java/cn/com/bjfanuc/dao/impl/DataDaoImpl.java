package cn.com.bjfanuc.dao.impl;

import cn.com.bjfanuc.App;
import cn.com.bjfanuc.dao.DataDao;
import cn.com.bjfanuc.exception.DataErrException;
import cn.com.bjfanuc.utils.ReturnValue;
import cn.com.bjfanuc.utils.TaosUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public class DataDaoImpl implements DataDao {
    private List<String> hosts = App.taosHosts;
    private List<TaosUtil> taosUtils = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(DataDaoImpl.class);
    private String database = App.taos == null ? null : App.taos.elementText("database");

    /**
     * taosUtil根据taosHost个数创建，并以对应的taosHost初始化，实现多连接功能
     */
    public DataDaoImpl() {
        if (hosts == null || hosts.size() == 0) {
            hosts.add("127.0.0.1");
        }

        for (int i = 0; i < hosts.size(); i++) {

            taosUtils.add(new TaosUtil(hosts.get(i)));
        }
        if (database == null || "".equals(database)) {
            logger.error("database is null!");
            System.exit(1);
        }
    }


    @Override
    public int saveData(JSONObject jsonData, String tableName) throws DataErrException, SQLException {
        return saveDataWhthoutPrepare(jsonData, tableName);
    }

    /**
     * 在taos支持SQL预编译之前使用此方法进行SQL语句的拼接存储
     *
     * @param jsonData
     * @param tableName
     * @return
     * @throws DataErrException
     * @throws SQLException
     */
    public int saveDataWhthoutPrepare(JSONObject jsonData, String tableName) throws DataErrException, SQLException {
        Set<String> properties = jsonData.keySet();
        Set<Map.Entry<String, Object>> entries = jsonData.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        StringBuffer sql = new StringBuffer("import into ").append(database).append(".").append(tableName);
        String col = properties.toString().replaceAll("\\[", "").replaceAll("\\]", "");
        sql.append("(").append(col).append(")");
        sql.append(" values (");
        int i = 0;
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (i == 0) {
                sql.append("'").append(next.getValue()).append("'");

            } else {

                sql.append(",'").append(next.getValue()).append("'");
            }
            i++;

        }
        sql.append(")");
        int val = -1;
        try {

            val = taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString()); //对应线程使用对应线程下的taosUtil进行存储
        } catch (SQLException e) {
            if (e.getErrorCode() == ReturnValue.INVALID_SQL) {
//                logger.error("invalid sql: " + sql);
                throw new DataErrException("data error: invalid data for this subCmd");
            } else
                throw e;
        } finally {
            properties = null;
            entries = null;
            iterator = null;
            sql = null;
            col = null;
        }
        return val;
    /*   else  if (val != 1 && val != ReturnValue.TABLE_NOT_EXIST)
            logger.error("insert failed SQL: return " + val + "\nSQL:" + sql); //可能没有这种可能，因为都通过异常来处理了
*/
    }

    /**
     * 创建表
     *
     * @param subCmd
     * @param tableName
     * @return
     * @throws SQLException
     */
    @Override
    public int createTableUsingTags(String subCmd, String tableName) throws SQLException {
        StringBuffer sql = new StringBuffer("create table ").append(database).append(".");


        sql.append(tableName).append(" using ").append(database).append(".mt_").append(subCmd)
                .append(" tags (")
                .append("'")
                .append(tableName)
                .append("')");
        int val = taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());
        sql = null;
        return val;

    }

    /**
     * 创建数据库
     *
     * @return
     * @throws SQLException
     */
    @Override
    public int createDatabase() throws SQLException {
        StringBuffer sql = new StringBuffer("create database if not exists " + database);
        int val = taosUtils.get(0).executeUpdateWithReconnect(sql.toString());
        sql = null;
        return val;

    }

    /**
     * 创建metric
     *
     * @param sqlSuffix
     * @param subCmd
     * @return
     * @throws SQLException
     */
    @Override
    public int createMertic(String sqlSuffix, String subCmd) throws SQLException {
        StringBuffer sql = new StringBuffer("create table  ").append(database);

        sql.append(".mt_");
        sql.append(subCmd);
        sql.append(sqlSuffix);
        sql.append(" tags(tableName binary(32))");
        int val = taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());
        sql = null;
        return val;

    }


}
