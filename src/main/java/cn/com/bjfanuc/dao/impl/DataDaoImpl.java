package cn.com.bjfanuc.dao.impl;

import cn.com.bjfanuc.App;
import cn.com.bjfanuc.dao.DataDao;
import cn.com.bjfanuc.exception.DataErrException;
import cn.com.bjfanuc.utils.TaosUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataDaoImpl implements DataDao {
    private List<String> hosts = App.taosHosts;
    private List<TaosUtil> taosUtils = new ArrayList<>();
    private Logger logger = LoggerFactory.getLogger(DataDaoImpl.class);

    public DataDaoImpl() {
        if (hosts == null || hosts.size() == 0) {
            hosts.add("127.0.0.1");
        }

        for (int i = 0; i < hosts.size(); i++) {

            taosUtils.add(new TaosUtil(hosts.get(i)));
        }
    }


    @Override
    public int saveData(JSONObject value) throws DataErrException {
        return saveDataWhthoutPrepare(value);
    }


    public int saveDataWhthoutPrepare(JSONObject value) throws DataErrException {
        JSONObject jsonData = value.getJSONObject("DATA");
        Set<String> properties = jsonData.keySet();
        Set<Map.Entry<String, Object>> entries = jsonData.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        String tableName = jsonData.getString("CNC_ID");
        StringBuffer sql = new StringBuffer("insert into " + tableName);
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
        int val = taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());
        if (val == 28) {
            throw new DataErrException("invalid column name");
        }
       else  if (val != 1 && val != 32)
            logger.error("insert failed SQL:" + sql);

        return val;
    }


    public int createTable(String sqlSuffix, String tableName) {

        StringBuffer sql = new StringBuffer("create table " + tableName);

        sql.append(sqlSuffix);
        int val = -1;
        val = taosUtils.get(Integer.parseInt(Thread.currentThread().getName())).executeUpdateWithReconnect(sql.toString());

        return val;
    }

    @Override
    public int createDatabase() {
        StringBuffer sql = new StringBuffer("create database if not exists fanuc_cnc_test");
        int val = -1;
        taosUtils.get(0).executeUpdateWithReconnect(sql.toString());
        return taosUtils.get(0).executeUpdateWithReconnect("use fanuc_cnc_test");
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
