package cn.com.bjfanuc.dao.impl;

import cn.com.bjfanuc.App;
import cn.com.bjfanuc.dao.TableInfoDao;
import cn.com.bjfanuc.exception.DataErrException;
import cn.com.bjfanuc.utils.RedisUtil;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableInfoDaoImpl implements TableInfoDao {

    private Map<String, String> subcmdMap = new HashMap<>();
    private List<RedisUtil> redisUtils = new ArrayList<>();

    private int count = 1;

    /**
     * 以taosHost数量为基准创建redisUtils数组，将每个线程隔离开
     */ {
        count = App.taosHosts.size();
        if (count == 0)
            count = 1;
        for (int i = 0; i < count; i++) {
            redisUtils.add(new RedisUtil());
        }
//       if (jedis.isConnected())


    }


    /**
     * 以subCmd为键获取建表语句后缀(create table tablename后的语句)
     * 先从Map中取，若不存在，则读取redis
     *
     * @param subCmd
     * @return
     * @throws DataErrException
     */
    @Override
    public String getSqlSuffix(String subCmd) throws DataErrException {
        if (subCmd == null)
            return null;
        if (subcmdMap.get(subCmd) == null) {
            redisUtils.get(Integer.parseInt(Thread.currentThread().getName())).get("SUBCMD_" + subCmd);
            subcmdMap.put(subCmd, redisUtils.get(Integer.parseInt(Thread.currentThread().getName())).get("SUBCMD_" + subCmd)); //对应线程使用对应线程下的redisUtil，线程名以数字命名
        }
        return subcmdMap.get(subCmd);

    }
}
