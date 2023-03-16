package org.dcu.database;

import java.util.Properties;

public class MoralisConnectionManager {

    public static final String TABLE_NFT_CONTRACTS = "nft_contracts";
    public static final String TABLE_NFT_OPEN_SEA_TRADES = "nft_open_sea_trades";

    //public static final String TABLE_NFT_TRANSFERS = "nft_transfers";
    public static final String TABLE_NFT_TRANSFERS = "krys_nft_transfer";

    private String driverClass = "com.mysql.cj.jdbc.Driver";

    private Properties props = new Properties();

    private String PUBLIC_JDBC_URL = "jdbc:mysql://35.193.69.26:3306/moralis";

    private String PRIVATE_JDBC_URL = "jdbc:mysql://10.27.64.3:3306/moralis";


    public MoralisConnectionManager() {
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
