package cn.com.bjfanuc.dao;

import cn.com.bjfanuc.exception.DataErrException;

public interface TableInfoDao {
    //获取建表语句后缀
    String getSqlSuffix(String subCmd) throws DataErrException;
}
