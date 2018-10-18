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

    public int createTableAndSaveData(JSONObject jsonObject) throws DataErrException {
        String sqlSuffix = tableInfoDao.getSqlSuffix(jsonObject.getString("SUBCMD"));
        String tableName = jsonObject.getJSONObject("DATA").getString("CNC_ID");
        int val = -1;
        if (sqlSuffix == null) {
            throw new DataErrException("invalid SUBCMD");
        }
        try {
            int table = dataDao.createTable(sqlSuffix, tableName);//这里改过了，不应该建表成功才插入，这样其中一个线程建表以后其他两个线程建表不成功就不存了
            val = dataDao.saveData(jsonObject);
        } catch (SQLException e) {
                        if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST)
                        logger.error("Table have been created but not exists: return " + tableName + "\n" + e.getMessage());
                        else{
                            logger.error(e.getMessage());
                        }
        }

//            Count.incCreateTableNum();

//            logger.error("create table failed");

        return val;

    }

    @Override
    public int save(JSONObject jsonObject) {

        int val = -1;

        try {
            String tableName = jsonObject.getJSONObject("DATA").getString("CNC_ID");
            if (tableName == null)
                throw new JSONException("invalid CNC_ID");
            try {

                val = dataDao.saveData(jsonObject);

            } catch (SQLException e){
               if (e.getErrorCode() == ReturnValue.TABLE_NOT_EXIST){
                   val = createTableAndSaveData(jsonObject);

               }

            }
            } catch (DataErrException e) {
            logger.error("Data error: " + e.getMessage());
        }catch (NullPointerException e){
            logger.error("Data error: property 'DATA' is null");
        }

        return val;

    }
    @Override
    public int createDatabase() throws SQLException {
      return  dataDao.createDatabase();
    }
}
