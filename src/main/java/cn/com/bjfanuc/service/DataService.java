package cn.com.bjfanuc.service;

import cn.com.bjfanuc.exception.DataErrException;
import com.alibaba.fastjson.JSONObject;

import java.sql.SQLException;

public interface DataService {
    int save(JSONObject jsonObject) throws DataErrException;

    int createDatabase() throws SQLException;
}
