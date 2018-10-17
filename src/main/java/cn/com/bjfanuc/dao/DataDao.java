package cn.com.bjfanuc.dao;

import cn.com.bjfanuc.exception.DataErrException;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;

public interface DataDao {
    int createTable(String sqlSuffix, String tableName) throws SQLException;



    int saveData(JSONObject value) throws DataErrException, SQLException;


    int createDatabase() throws SQLException;
}
