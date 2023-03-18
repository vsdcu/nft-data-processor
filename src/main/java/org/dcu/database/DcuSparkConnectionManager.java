package org.dcu.database;

import java.util.Properties;

public class DcuSparkConnectionManager {


    public  static final String TABLE_NFT_CONTRACT_ENTITY = "mrc_parsed_nft_contract_data";

    private String driverClass = "com.mysql.cj.jdbc.Driver";

    private Properties props = new Properties();

    private String url = "jdbc:mysql://35.193.69.26:3306/dcu_spark";  //us-db
    //private String url = "jdbc:mysql://35.197.248.253:3306/dcu_spark"; //eu-db

    public DcuSparkConnectionManager() {
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
//read_only
//DCU_123!!!