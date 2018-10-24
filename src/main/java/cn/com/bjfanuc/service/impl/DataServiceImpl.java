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

    /**
     * 创建metric并创建表，Service内部调用
     *
     * @param cncId
     * @param subCmd
     * @return
     * @throws DataErrException
     */
    private int createMetricAndCreateTable(String cncId, String subCmd) throws DataErrException {
        String sqlSuffix = tableInfoDao.getSqlSuffix(subCmd);
        if (sqlSuffix == null) {
            throw new DataErrException("create metric: invalid subCmd " + subCmd);
        }
        int val = -1;
        try {
            val = dataDao.createMertic(sqlSuffix, subCmd);
            logger.info("metric mt_" + subCmd + "created");
            val = createTable(subCmd, cncId);
        } catch (SQLException e) {
            logger.error("create metric mt_" + subCmd + "error: return: " + e.getErrorCode(), e);
        }
        return val;
    }

    /**
     * 创建表并执行存储，Service内部调用
     *
     * @param jsonData
     * @param cncId
     * @param subCmd
     * @return
     * @throws DataErrException
     */
    private int createTableAndSaveData(JSONObject jsonData, String cncId, String subCmd) throws DataErrException {
        int val = -1;
        int threadIndex = Thread.currentThread().getName().charAt(0) - 48;
        val = createTable(subCmd, cncId);
        try {

            Count.createTableNum[threadIndex]++;
            val = saveData(jsonData, cncId);

        } catch (SQLException e) {


            logger.error("save data error return: " + e.getErrorCode(), e);


        }

//            Count.incCreateTableNum();

//            logger.error("create table failed");

        return val;

    }

    /**
     * 创建表，Service内部调用
     *
     * @param subCmd
     * @param cncId
     * @return
     * @throws DataErrException
     */
    private int createTable(String subCmd, String cncId) throws DataErrException {
        int val = -1;
        try {
            val = dataDao.createTableUsingTags(subCmd, cncId);
            logger.info("table " + cncId + " created ");
        } catch (SQLException e) {
            if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) {  //若metric不存在创建metric并使用metric创建表
                logger.info("create metric mt_" + subCmd);
                val = createMetricAndCreateTable(cncId, subCmd);

            } else {
                logger.error("create table " + cncId + " error. return: " + e.getErrorCode(), e);
            }
        }

        return val;
    }

    /**
     * 存储的Service封装，用于Service内部调用
     *
     * @param data
     * @param tableName
     * @return
     * @throws DataErrException
     * @throws SQLException
     */
    private int saveData(JSONObject data, String tableName) throws DataErrException, SQLException {
        int threadIndex = Thread.currentThread().getName().charAt(0) - 48;

        int val = dataDao.saveData(data, tableName);

        if (val > 0) {
            Count.saveSuccess[threadIndex]++;
        }

        return val;

    }

    /**
     * Service接口中的存储方法,用于外部调用
     *
     * @param jsonObject
     * @return
     * @throws DataErrException
     */
    @Override
    public int save(JSONObject jsonObject) throws DataErrException {


        int val = -1;
        String subCmd = null;
        JSONObject dataBuffer = null;
        String tableName = null;
        try {
            subCmd = jsonObject.getString("SUBCMD");
            if (subCmd == null) {

                throw new DataErrException("invalid SubCmd " + subCmd);
            }
            dataBuffer = jsonObject.getJSONObject("DATA");
            tableName = dataBuffer.getString("FANUC_CNC".equals(subCmd) ? "CNC_ID" : "DEV_ID");
            if (tableName == null)
                throw new DataErrException("invalid ID " + tableName);


            val = saveData(dataBuffer, tableName);

        } catch (SQLException e) {
            if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST) { //若表不存在，则建表并存储

                logger.info("create table " + tableName);
                val = createTableAndSaveData(dataBuffer, tableName, subCmd);
            } else {
                logger.error("save failed: return " + e.getErrorCode(), e);  //其他SQLException
            }


        } catch (NullPointerException e) {
            logger.error("Data error: Json or property 'DATA' is null" + "\n", e);
        } finally {
            subCmd = null;
            dataBuffer = null;
            tableName = null;
        }

        return val;

    }

    /**
     * 创建数据库
     *
     * @return
     * @throws SQLException
     */
    @Override
    public int createDatabase() throws SQLException {
        return dataDao.createDatabase();
    }
}
