/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;

public class JdbcPool {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private HikariDataSource ds;
    private Configuration config;

    public JdbcPool(String urlPattern, String driverClassName, Configuration config, Field... variables) {
        this.config = config;
        LOGGER.trace("Config: {}", config.asProperties());
        Properties props = config.asProperties();
        Field[] varsWithDefaults = JdbcConnection.combineVariables(variables,
                                                    JdbcConfiguration.HOSTNAME,
                                                    JdbcConfiguration.PORT,
                                                    JdbcConfiguration.USER,
                                                    JdbcConfiguration.PASSWORD,
                                                    JdbcConfiguration.DATABASE);
        String url = JdbcConnection.findAndReplace(urlPattern, props, varsWithDefaults);
        LOGGER.trace("driverClassName: {}", driverClassName);
        LOGGER.trace("Props: {}", props);
        LOGGER.trace("URL: {}", url);

        HikariConfig hconfig = new HikariConfig();
        hconfig.setJdbcUrl(url);
        hconfig.setDriverClassName(driverClassName);
        hconfig.setUsername(config.getString(JdbcConfiguration.USER));
        hconfig.setPassword(config.getString(JdbcConfiguration.PASSWORD));
        hconfig.setMaximumPoolSize(16);
        hconfig.setMinimumIdle(0);
        hconfig.setIdleTimeout(300000);
        hconfig.setMaxLifetime(300000);
        ds = new HikariDataSource(hconfig);
    }

    public JdbcConnection getConnection() {
        return new JdbcConnection(this.config, poolFactory());
    }

    public ConnectionFactory poolFactory() {
        return (config) -> {
            return ds.getConnection();
        };
    }

    public void close() {
        this.ds.close();
    }
    
}