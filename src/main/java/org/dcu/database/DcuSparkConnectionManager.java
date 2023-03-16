package org.dcu.database;

import java.util.Properties;

public class DcuSparkConnectionManager {


    public  static final String TABLE_NFT_CONTRACT_ENTITY = "nft_contract_entity";

    private String driverClass = "com.mysql.cj.jdbc.Driver";

    private Properties props = new Properties();

    //private String url = "jdbc:mysql://35.193.69.26:3306/dcu_spark";
    // jdbc:mysql://35.193.69.26:3306/moralis
    private String PUBLIC_JDBC_URL = "jdbc:mysql://35.193.69.26:3306/dcu_spark";

    private String PRIVATE_JDBC_URL = "jdbc:mysql://10.27.65.5:3306/dcu_spark";

    public DcuSparkConnectionManager() {
        // define JDBC connection properties

        props.setProperty("driver", driverClass);
        props.setProperty("connectTimeout", "30000");
        props.setProperty("socketTimeout", "300000");

        props.setProperty("user", "root");
        props.setProperty("password", "!!DCU_Cloud_SYS_2023!!");
    }

    public String getUrl() {
        return PUBLIC_JDBC_URL;
    }

    public Properties getProps() {
        return props;
    }

}
