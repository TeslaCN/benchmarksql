/*
 * ShardingJdbc - this use to get connect for sharding jdbc
 *
 * Copyright (C) 2021 justbk
 *
 */


import org.apache.log4j.Logger;
import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class ShardingJdbc {
    private static org.apache.log4j.Logger log = Logger.getLogger(ShardingJdbc.class);
    public static final String DEFAULT_CONFIG = "config-sharding.yaml";
    public static  ConcurrentHashMap<String, DataSource> mapDataSource = new ConcurrentHashMap<>(1);
    public static Object createDataSourceLock = new int[0];

    public static DataSource getDataSource(String configFile) {
        if (mapDataSource.containsKey(configFile)) {
            return mapDataSource.get(configFile);
        }
        synchronized (createDataSourceLock) {
            if (mapDataSource.contains(configFile)) {
                return mapDataSource.get(configFile);
            }
            if (configFile == null || "".equals(configFile)) {
                configFile = DEFAULT_CONFIG;
            }
            File file = new File(configFile);
            if (!file.isAbsolute()) {
                String fileAbsPath = ShardingJdbc.class.getClassLoader().getResource(configFile).getFile();
                file = new File(fileAbsPath);
            }
            try {
                DataSource dataSource =  YamlShardingSphereDataSourceFactory.createDataSource(file);
                mapDataSource.put(configFile, dataSource);
                mapDataSource.put(file.getAbsolutePath(), dataSource);
                return dataSource;
            } catch (Exception exp) {
                exp.printStackTrace();
                return null;
            }
        }

    }

    public static synchronized Connection getConnection(String dbConn, Properties dbProperties) throws SQLException {
        if (dbConn.toLowerCase(Locale.ENGLISH).contains("opengauss")) {
            log.error("create in sharding: this connection use normal connector!!!" + dbConn);
            DataSource dataSource = getDataSource((String) dbProperties.getOrDefault("config", ""));
            return dataSource.getConnection();
        } else {
            log.error("error in sharding: this connection use normal connector!!!" + dbConn);
            return DriverManager.getConnection(dbConn, dbProperties);
        }
    }
}
