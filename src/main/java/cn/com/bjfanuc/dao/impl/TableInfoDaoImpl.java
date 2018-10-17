package cn.com.bjfanuc.dao.impl;

import cn.com.bjfanuc.App;
import cn.com.bjfanuc.dao.TableInfoDao;
import cn.com.bjfanuc.exception.DataErrException;
import cn.com.bjfanuc.utils.RedisUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableInfoDaoImpl implements TableInfoDao {

    private Map<String,String> subcmdMap = new HashMap<>();
    private List<RedisUtil> redisUtils = new ArrayList<>();
    private int count = 1;
    {
        count = App.taosHosts.size();
        if (count == 0)
            count = 1;
        for (int i=0; i<count; i++){
            redisUtils.add(new RedisUtil());
        }
//       if (jedis.isConnected())

    }



    @Override
    public String  getSqlSuffix(String subCmd) throws DataErrException {
        if (subCmd == null)
            return null;
            if (subcmdMap.get(subCmd) == null){
              redisUtils.get(Integer.parseInt(Thread.currentThread().getName())).get("SUBCMD_" + subCmd);
                subcmdMap.put(subCmd,redisUtils.get(Integer.parseInt(Thread.currentThread().getName())).get("SUBCMD_" + subCmd));
            }
            return subcmdMap.get(subCmd);

    }
}
