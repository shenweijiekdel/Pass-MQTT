package cn.com.bjfanuc.dao;

import cn.com.bjfanuc.exception.DataErrException;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;

public interface DataDao {
    //存储数据
    int saveData(JSONObject jsonData, String tableName) throws DataErrException, SQLException;

    //使用Metric时创建表
    int createTableUsingTags(String tableName, String metricName) throws SQLException;

    //创建数据库
    int createDatabase() throws SQLException;

    //创建metric
    int createMertic(String tableName, String subCmd) throws SQLException;
}
