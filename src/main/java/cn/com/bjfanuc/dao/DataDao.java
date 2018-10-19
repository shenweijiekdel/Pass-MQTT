package cn.com.bjfanuc.dao;

import cn.com.bjfanuc.exception.DataErrException;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;

public interface DataDao {
    int saveData(JSONObject jsonData, String tableName) throws DataErrException, SQLException;

//    int createTable(String sqlSuffix, String tableName) throws SQLException;


    //使用Metric时创建Metric
    int createTableUsingTags(String tableName, String metricName) throws SQLException;

    int createDatabase() throws SQLException;

    int createMertic(String tableName, String subCmd) throws SQLException;
}
