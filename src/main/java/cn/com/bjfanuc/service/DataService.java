package cn.com.bjfanuc.service;

import com.alibaba.fastjson.JSONObject;

public interface DataService {
    int save(JSONObject jsonObject);

    int createDatabase();
}
