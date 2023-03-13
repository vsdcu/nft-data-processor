package org.dcu.database;

import java.util.Properties;

public class MoralisConnectionManager {

    public static final String TABLE_NFT_CONTRACTS = "krys_nft_contracts";
    public static final String TABLE_NFT_OPEN_SEA_TRADES = "krys_nft_open_sea_trades";
    public static final String TABLE_NFT_TRANSFERS = "krys_nft_transfer";

    private String driverClass = "com.mysql.cj.jdbc.Driver";

    private Properties props = new Properties();

    private String url = "jdbc:mysql://35.193.69.26:3306/moralis";

    public MoralisConnectionManager() {
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
