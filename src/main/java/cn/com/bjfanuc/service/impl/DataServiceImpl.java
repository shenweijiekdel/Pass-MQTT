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
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class DataServiceImpl implements DataService {
    private DataDao dataDao = SingletonFactory.getBean(DataDaoImpl.class.getName());
    private TableInfoDao tableInfoDao = SingletonFactory.getBean(TableInfoDaoImpl.class.getName());
    private Logger logger = LoggerFactory.getLogger(DataServiceImpl.class);


    public int createMetricAndCreateTable(String tableName, String subCmd) throws DataErrException {
        String sqlSuffix = tableInfoDao.getSqlSuffix(subCmd);
        if (sqlSuffix == null) {
            throw new DataErrException("create metric: invalid subCmd " + subCmd);
        }
        int val = -1;
        try {
            val = dataDao.createMertic(sqlSuffix, subCmd);
            logger.info("metric mt_" + subCmd + " created");
            val = createTable(subCmd, tableName);
        } catch (SQLException e) {
            logger.error("create metric mt_" + subCmd + "error: return: " + e.getErrorCode(), e);
        }
        return val;
    }

    public int createTableAndSaveData(JSONObject jsonData, String tableName, String subCmd) throws DataErrException {
        int val = -1;

        val = createTable(subCmd, tableName);
        try {

            Count.createTableNum[Integer.parseInt(Thread.currentThread().getName())]++;
            val = saveData(jsonData, tableName);

        } catch (SQLException e) {


            logger.error("save data error return: " + e.getErrorCode(), e);


        }

//            Count.incCreateTableNum();

//            logger.error("create table failed");

        return val;

    }

    public int createTable(String subCmd, String tableName) throws DataErrException {
        int val = -1;
        try {
            val = dataDao.createTableUsingTags(subCmd, tableName);
            logger.info("table " + tableName + " created ");
        } catch (SQLException e) {
            if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) {
                logger.info("create metric mt_" + subCmd);
                val = createMetricAndCreateTable(tableName, subCmd);

            } else {
                logger.error("create table " + tableName + " error. return: " + e.getErrorCode(), e);
            }
        }

        return val;
    }

    private int saveData(JSONObject data, String tableName) throws DataErrException, SQLException {

        int val = dataDao.saveData(data, tableName);

        if (val > 0) {
            Count.saveSuccess[Integer.parseInt(Thread.currentThread().getName())]++;
        }

        return val;

    }

    @Override
    public int save(JSONObject jsonObject) throws DataErrException{

        int val = -1;
        String subCmd = null;
        JSONObject dataBuffer = null;
        String tableName = null;
        try {
            subCmd = jsonObject.getString("SUBCMD");
            dataBuffer = jsonObject.getJSONObject("DATA");
            tableName = dataBuffer.getString("FANUC_CNC".equals(subCmd) ? "CNC_ID" : "DEV_ID");
            if (tableName == null)
                throw new DataErrException("invalid tableName ");
            if (subCmd == null) {

                throw new DataErrException("invalid SubCmd " + subCmd);
            }


            val = saveData(dataBuffer, tableName);

        } catch (SQLException e) {
            if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) {
                logger.info("create table " + tableName);
                val = createTableAndSaveData(dataBuffer, tableName, subCmd);
            } else {
                logger.error("save failed: return " + e.getErrorCode(), e);
            }


        } catch (NullPointerException e) {
            logger.error("Data error: Json or property 'DATA' is null" + "\n", e);
        }

        return val;

    }

    @Override
    public int createDatabase() throws SQLException {
        return dataDao.createDatabase();
    }
}
