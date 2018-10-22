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


    public int saveDataWhthoutPrepare(JSONObject jsonData, String tableName) throws DataErrException, SQLException {
        Set<String> properties = jsonData.keySet();
        Set<Map.Entry<String, Object>> entries = jsonData.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        StringBuffer sql = new StringBuffer("insert into ").append(database).append(".").append(tableName);
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
        try {

         return taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());
        }catch (SQLException e){
            if (e.getErrorCode() == ReturnValue.INVALID_SQL){
                logger.error("invalid sql: " + sql);
            } else
                throw e;
        }
    /*   else  if (val != 1 && val != ReturnValue.TABLE_NOT_EXIST)
            logger.error("insert failed SQL: return " + val + "\nSQL:" + sql); //可能没有这种可能，因为都通过异常来处理了
*/
        return -1;
    }
    //使用Metric时创建Metric
    @Override
    public int createTableUsingTags(String subCmd, String tableName) throws SQLException {
        StringBuffer sql = new StringBuffer("create table ").append(database).append(".");
        sql.append(tableName).append(" using ").append(database).append(".mt_").append(subCmd)
                .append(" tags (")
                .append("'")
                .append(tableName)
                .append("')");
        return taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());

    }
   /* public int createTable(String sqlSuffix, String tableName) throws SQLException {

        StringBuffer sql = new StringBuffer("create table  ");
        sql.append(sqlSuffix);

        return taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());
    }*/

    @Override
    public int createDatabase() throws SQLException {
        StringBuffer sql = new StringBuffer("create database if not exists " + database);

        return   taosUtils.get(0).executeUpdateWithReconnect(sql.toString());
    }

    @Override
    public int createMertic(String sqlSuffix, String subCmd) throws SQLException {
        StringBuffer sql = new StringBuffer("create table  ").append(database);
        sql.append(".mt_");
        sql.append(subCmd);
        sql.append(sqlSuffix);
        sql.append(" tags(tableName binary(32))");
        return taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());
    }

  /*  @Override
    public int saveData(JSONObject value) {   //没加事务
        JSONObject jsonData = value.getJSONObject("DATA");
        Set<String> properties = jsonData.keySet();
        Set<Map.Entry<String, Object>> entries = jsonData.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        String tableName = jsonData.getString("CNC_ID");
        StringBuffer sql = new StringBuffer("insert into " + tableName);
        sql.append(properties.toString().replaceAll("\\[", "\\(").replaceAll("\\]", "\\)"));
        sql.append(" values (");
        for (int i = 0; i < properties.size(); i++) {
            if (i == 0)
                sql.append("?");
            else
                sql.append(",?");
        }
        sql.append(")");
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connections.get(0).prepareStatement(sql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
//         System.out.println("sql: " + sql);
        int i = 0;
        Object args[] = new Object[properties.size()];
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
//               args[i] = next.getValue();
            try {
                preparedStatement.setString(i, next.getValue().toString());
            } catch (SQLException e) {
                e.printStackTrace();
            }
            i++;
        }

        try {
            return preparedStatement.executeUpdate(sql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return -1;
    }*/


   /* private Class getDevanningType(String name){
        Class<?> aClass = null;
        try {
            aClass = Class.forName(name);

            Field type = aClass.getDeclaredField("TYPE");
            Method[] methods = aClass.getMethods();
            Method method = null;
            for (Method m :methods
            ) {
                if (m.getName().startsWith("parse")){
                    method = m;

                    break;
                }
            }
            return  method.getReturnType();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (NoSuchFieldException e) {
            return aClass;
        }
    }*/
}
