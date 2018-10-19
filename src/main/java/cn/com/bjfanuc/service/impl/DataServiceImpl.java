package cn.com.bjfanuc.service.impl;

import cn.com.bjfanuc.Count;
import cn.com.bjfanuc.dao.DataDao;
import cn.com.bjfanuc.dao.TableInfoDao;
import cn.com.bjfanuc.dao.impl.DataDaoImpl;
import cn.com.bjfanuc.dao.impl.TableInfoDaoImpl;
import cn.com.bjfanuc.exception.DataErrException;
import cn.com.bjfanuc.service.DataService;
import cn.com.bjfanuc.utils.ReturnValue;
import cn.com.bjfanuc.utils.SingletonFactory;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.sql.SQLException;


public class DataServiceImpl implements DataService {
    private DataDao dataDao = SingletonFactory.getBean(DataDaoImpl.class.getName());
    private TableInfoDao tableInfoDao = SingletonFactory.getBean(TableInfoDaoImpl.class.getName());
    private Logger logger = LoggerFactory.getLogger(DataServiceImpl.class);

    public int createMetricAndCreateTable(String tableName, String subCmd) throws DataErrException {
        String sqlSuffix = tableInfoDao.getSqlSuffix(subCmd);
        if (sqlSuffix == null)
            throw new DataErrException("invalid subCmd " + subCmd);
        int val = -1;
        try {
            dataDao.createMertic(sqlSuffix, subCmd);
            val = dataDao.createTableUsingTags(tableName, subCmd);
        } catch (SQLException e) {
            logger.error(e.getMessage() + " return: " + e.getErrorCode());
        }
        return val;
    }

    public int createTableAndSaveData(JSONObject jsonData, String tableName, String subCmd) throws DataErrException {
        int val = -1;

        try {
            createTable(subCmd, tableName);//这里改过了，不应该建表成功才插入，这样其中一个线程建表以后其他两个线程建表不成功就不存了
            val = dataDao.saveData(jsonData, tableName);
        } catch (SQLException e) {
            logger.error(e.getMessage());

        }

//            Count.incCreateTableNum();

//            logger.error("create table failed");

        return val;

    }

    public int createTable(String subCmd, String tableName) throws SQLException, DataErrException {
        int val = -1;
        try {
            dataDao.createTableUsingTags(subCmd, tableName);

        } catch (SQLException e) {
            if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) {
                val = createMetricAndCreateTable(tableName, subCmd);

            } else {
                logger.error(e.getMessage() + " return: " + e.getErrorCode());
            }
        }

        return val;
    }

    @Override
    public int save(JSONObject jsonObject) {

        int val = -1;

        try {
            JSONObject data = jsonObject.getJSONObject("DATA");
            String subCmd = jsonObject.getString("SUBCMD");
            String tableName = data.getString("FANUC_CNC".equals(subCmd) ? "CNC_ID" : "DEV_ID");
            if (tableName == null)
                throw new DataErrException("invalid SubCmd");
            try {

                val = dataDao.saveData(data, tableName);

            } catch (SQLException e) {
                if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) {
                    val = createTableAndSaveData(data, tableName, subCmd);

                }

            }
        } catch (DataErrException e) {
            logger.error("Data error: " + e.getMessage());
        } catch (NullPointerException e) {
            logger.error("Data error: property 'DATA' is null");
        }

        return val;

    }

    @Override
    public int createDatabase() throws SQLException {
        return dataDao.createDatabase();
    }
}
