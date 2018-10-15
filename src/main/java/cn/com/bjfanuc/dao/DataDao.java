package cn.com.bjfanuc.dao;

import cn.com.bjfanuc.exception.DataErrException;
import com.alibaba.fastjson.JSONObject;

public interface DataDao {
    int createTable(String sqlSuffix, String tableName);



    int saveData(JSONObject value) throws DataErrException;


    int createDatabase();
}
