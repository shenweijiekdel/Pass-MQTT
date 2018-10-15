package cn.com.bjfanuc.domain;

import java.util.Map;
import java.util.TreeMap;

public class TableInfo {
 private String name;
 private String dbType;
 private String constraint;
 private String javaType;
    public TableInfo(){}
    public TableInfo(String name, String dbType, String javaType, String constraint) {
        this.name = name;
        this.dbType = dbType;
        this.constraint = constraint;
        this.javaType = javaType;
    }
    public TableInfo(String name, String dbType, String javaType) {
        this.name = name;
        this.dbType = dbType;
        this.javaType = javaType;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDbType() {
        return dbType;
    }

    public String getJavaType() {
        return javaType;
    }

    public void setJavaType(String javaType) {
        this.javaType = javaType;
    }

    public void setDbType(String type) {
        this.dbType = type;
    }

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
    }

    /*public TypeMap() {
        dbTypeToJavatype.put("varchar","java.lang.String");
        dbTypeToJavatype.put("char","java.lang.String");
        dbTypeToJavatype.put("int","int");
        dbTypeToJavatype.put("smallint","short");
        dbTypeToJavatype.put("float","float");

    }*/

    @Override
    public String toString() {
        return "DataColumn{" +
                "name='" + name + '\'' +
                ", dbType='" + dbType + '\'' +
                ", constraint='" + constraint + '\'' +
                ", javaType='" + javaType + '\'' +
                '}';
    }
}
