package cn.com.bjfanuc;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class A {
    public static void main(String[] args) {

            PropertyConfigurator.configure("log4j.properties");

        Logger logger = Logger.getLogger(A.class);
        System.out.println(logger.getClass().getResource(""));
        logger.info("abc");
    }
}
