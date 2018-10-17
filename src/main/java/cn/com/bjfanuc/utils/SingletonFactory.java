package cn.com.bjfanuc.utils;

import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SingletonFactory {
    private static  Map<String,Object> map = new ConcurrentHashMap<>();
    private SingletonFactory(){}
    private static  Logger logger = Logger.getLogger(SingletonFactory.class);
    public static<T> T getBean(String name) {
        Object value = map.get(name);
            Object o = null;
        if (value == null) {
            try {
                Class<?> forName = Class.forName(name);
                o = forName.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                logger.error(e);
                return null;
            }
            map.put(name, o);
        }
        for (Map.Entry<String,Object> entry:map.entrySet()){
        }
            return (T) o;
    }


}
