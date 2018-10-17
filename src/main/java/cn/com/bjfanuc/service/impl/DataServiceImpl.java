package cn.com.bjfanuc.service.impl;

import cn.com.bjfanuc.Count;
import cn.com.bjfanuc.dao.DataDao;
import cn.com.bjfanuc.dao.TableInfoDao;
import cn.com.bjfanuc.dao.impl.DataDaoImpl;
import cn.com.bjfanuc.dao.impl.TableInfoDaoImpl;
import cn.com.bjfanuc.exception.DataErrException;
import cn.com.bjfanuc.service.DataService;
import cn.com.bjfanuc.utils.SingletonFactory;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;


public class DataServiceImpl implements DataService {
    private DataDao dataDao = SingletonFactory.getBean(DataDaoImpl.class.getName());
    private TableInfoDao tableInfoDao = SingletonFactory.getBean(TableInfoDaoImpl.class.getName());
    private Logger logger = LoggerFactory.getLogger(DataServiceImpl.class);

    public int createTableAndSaveData(JSONObject jsonObject) throws DataErrException {
        String sqlSuffix = tableInfoDao.getSqlSuffix(jsonObject.getString("SUBCMD"));
        String tableName = jsonObject.getJSONObject("DATA").getString("CNC_ID");

        if (sqlSuffix == null) {
            throw new DataErrException("SUBCMD is not exist");
        }
        dataDao.createTable(sqlSuffix, tableName);   //这里改过了，不应该建表成功才插入，这样其中一个线程建表以后其他两个线程建表不成功就不存了

//            Count.incCreateTableNum();

//            logger.error("create table failed");

        return dataDao.saveData(jsonObject);

    }

    @Override
    public int save(JSONObject jsonObject) {

        int val = -1;

        try {
            String tableName = jsonObject.getJSONObject("DATA").getString("CNC_ID");
            if (tableName == null)
                throw new JSONException("CNC_ID is null！");
            val = dataDao.saveData(jsonObject);


            if (val == 32) { //表不存在
                val = createTableAndSaveData(jsonObject);
            }

            } catch (DataErrException e) {
//            logger.error("Data error: " + e.getMessage());
        }catch (NullPointerException e){
            logger.error("Data error: property 'DATA' is null");
        }

        return val;

    }
    @Override
    public int createDatabase(){
      return  dataDao.createDatabase();
    }
}
