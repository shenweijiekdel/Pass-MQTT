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
            logger.info("create metric return " + val);
            val = createTable(subCmd, tableName);
        } catch (SQLException e) {
            logger.error("error return: " + e.getErrorCode(),e);
        }
        return val;
    }

    public int createTableAndSaveData(JSONObject jsonData, String tableName, String subCmd) throws DataErrException {
        int val = -1;

        try {
            val = createTable(subCmd, tableName);//这里改过了，不应该建表成功才插入，这样其中一个线程建表以后其他两个线程建表不成功就不存了

            Count.createTableNum[Integer.parseInt(Thread.currentThread().getName())] ++;
            val = saveData(jsonData, tableName);

        } catch (SQLException e) {


            logger.error("error return: " + e.getErrorCode(),e);


        }

//            Count.incCreateTableNum();

//            logger.error("create table failed");

        return val;

    }

    public int createTable(String subCmd, String tableName) throws SQLException, DataErrException {
        int val = -1;
        try {
            val = dataDao.createTableUsingTags(subCmd, tableName);
            logger.info("create table return " + val);
        } catch (SQLException e) {
            if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) {
                logger.info("invalid metric mt_" + subCmd + ", create it");
                val = createMetricAndCreateTable(tableName, subCmd);

            } else {
                logger.error("create table error: return: " + e.getErrorCode(),e);
            }
        }

        return val;
    }

    private int saveData(JSONObject data, String tableName) throws DataErrException, SQLException {
        int val = -1;
        val = dataDao.saveData(data, tableName);
        if (val == 0){
            throw new DataErrException("data cannot be saved,return 0");
        }
        Count.saveSuccess[Integer.parseInt(Thread.currentThread().getName())] ++;
        return val;

    }

    @Override
    public int save(JSONObject jsonObject) {

        int val = -1;

        String subCmd = jsonObject.getString("SUBCMD");
        JSONObject dataBuffer = jsonObject.getJSONObject("DATA");
            String tableName = dataBuffer.getString("FANUC_CNC".equals(subCmd) ? "CNC_ID" : "DEV_ID");
        try {
            if (tableName == null)
                throw new DataErrException("invalid tableName ");
            if (subCmd == null) {

                throw new DataErrException("invalid SubCmd " + subCmd);
            }
            try {

                val = saveData(dataBuffer, tableName);

            } catch (SQLException e) {
                if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) {
                    logger.info("invalid table " + tableName + ",creating it.");
                    val = createTableAndSaveData(dataBuffer, tableName, subCmd);
                }
                else {
                    logger.error("save failed: return " + e.getErrorCode(),e);
                }

            }
        } catch (DataErrException e) {
            logger.error("Data error: " + e.getMessage() + "\n.json: " + jsonObject ,e);
        } catch (NullPointerException e) {
            logger.error("Data error: property 'DATA' is null" + "\n",e);
        } catch (Exception e){
            logger.error("other error: ",e);
        }

        return val;

    }

    @Override
    public int createDatabase() throws SQLException {
        return dataDao.createDatabase();
    }
}
