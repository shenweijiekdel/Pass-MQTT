package cn.com.bjfanuc.service;

import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;

public interface DataService {
    int save(JSONObject jsonObject);

    int createDatabase() throws SQLException;
}
