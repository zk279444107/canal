package com.alibaba.otter.canal.client.adapter.es.test;

import java.sql.SQLException;

import com.alibaba.druid.pool.DruidDataSource;

public class TestConstant {

    public final static String    jdbcUrl      = "jdbc:mysql://10.200.53.65:3306/bbg_plat_trade?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&zeroDateTimeBehavior=convertToNull&useAffectedRows=true";
    public final static String    jdbcUser     = "sys_market_test";
    public final static String    jdbcPassword = "sys_market_test";

    public final static String    esHosts      = "10.200.53.24:9300,10.200.53.34:9300";
    public final static String    clusterName  = "bbg-es";

    public final static DruidDataSource dataSource;

    static {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(jdbcUser);
        dataSource.setPassword(jdbcPassword);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(1);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
        dataSource.setValidationQuery("select 1");
        try {
            dataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
