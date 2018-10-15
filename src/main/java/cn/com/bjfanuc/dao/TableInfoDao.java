package cn.com.bjfanuc.dao;

import cn.com.bjfanuc.exception.DataErrException;

public interface TableInfoDao {
//    void createTable();

    String  getSqlSuffix(String subCmd) throws DataErrException;
}
