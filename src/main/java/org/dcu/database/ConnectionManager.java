package org.dcu.database;

import java.util.Properties;

public class ConnectionManager {

    private String driverClass = "com.mysql.cj.jdbc.Driver";

    private Properties props = new Properties();

    private String url = "jdbc:mysql://35.193.69.26:3306/";

    public ConnectionManager() {
        // define JDBC connection properties

        props.setProperty("driver", driverClass);
        props.setProperty("user", "root");
        props.setProperty("password", "!!DCU_Cloud_SYS_2023!!");
    }

    public String getUrl() {
        return url;
    }

    public Properties getProps() {
        return props;
    }

}
