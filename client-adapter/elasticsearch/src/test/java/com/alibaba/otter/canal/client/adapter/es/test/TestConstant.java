package com.alibaba.otter.canal.client.adapter.es.test;

import java.sql.SQLException;

import com.alibaba.druid.pool.DruidDataSource;

public class TestConstant {

	public final static String jdbcUrl = "jdbc:mysql://10.200.53.65:3306/bbg_plat_trade?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&zeroDateTimeBehavior=convertToNull&useAffectedRows=true";
	public final static String jdbcUser = "sys_market_test";
	public final static String jdbcPassword = "sys_market_test";

	public final static String esHosts = "10.200.53.142:9300";
	public final static String clusterName = "bbg-es";

	public final static DruidDataSource dataSource;
	public final static DruidDataSource goods_dataSource;
	public final static DruidDataSource store_dataSource;

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
        
        goods_dataSource = new DruidDataSource();
        goods_dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        goods_dataSource.setUrl("jdbc:mysql://10.200.53.65:3306/bbg_plat_goods?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&zeroDateTimeBehavior=convertToNull&useAffectedRows=true");
        goods_dataSource.setUsername("sys_market_test");
        goods_dataSource.setPassword("sys_market_test");
        goods_dataSource.setInitialSize(1);
        goods_dataSource.setMinIdle(1);
        goods_dataSource.setMaxActive(5);
        goods_dataSource.setMaxWait(60000);
        goods_dataSource.setTimeBetweenEvictionRunsMillis(60000);
        goods_dataSource.setMinEvictableIdleTimeMillis(300000);
        goods_dataSource.setPoolPreparedStatements(false);
        goods_dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
        goods_dataSource.setValidationQuery("select 1");
        try {
        	goods_dataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        store_dataSource = new DruidDataSource();
        store_dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        store_dataSource.setUrl("jdbc:mysql://10.200.53.144:3306/bbg_plat_sp?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true&zeroDateTimeBehavior=convertToNull&useAffectedRows=true");
        store_dataSource.setUsername("selected_preview");
        store_dataSource.setPassword("selected_preview");
        store_dataSource.setInitialSize(1);
        store_dataSource.setMinIdle(1);
        store_dataSource.setMaxActive(5);
        store_dataSource.setMaxWait(60000);
        store_dataSource.setTimeBetweenEvictionRunsMillis(60000);
        store_dataSource.setMinEvictableIdleTimeMillis(300000);
        store_dataSource.setPoolPreparedStatements(false);
        store_dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
        store_dataSource.setValidationQuery("select 1");
        try {
        	goods_dataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
