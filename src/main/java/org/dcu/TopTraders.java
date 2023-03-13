package org.dcu;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_TRANSFERS;

/**
 * This will persist output in two tables
 *
 * 1. buyers_trades_count
 * 2. sellers_trades_count
 *
 * that can be used to create two metrics
 *
 * 1. Top buyers
 * 2. Top sellers
 */
public class TopTraders {

    public static final MoralisConnectionManager MORALIS_CONNECTION_MANAGER = new MoralisConnectionManager();
    public static final DcuSparkConnectionManager DCU_SPARK_CONNECTION_MANAGER = new DcuSparkConnectionManager();
    private static String tableName = TABLE_NFT_TRANSFERS;

    public static void findTopBuyers(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding Top Buyers from table: " + tableName);
        Dataset<Row> rowDataset = spark.read()
                .jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableName, MORALIS_CONNECTION_MANAGER.getProps())
                .select(col("to_address").as("buyer_address"))
                .groupBy(col("buyer_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "buyers_trades_count", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //nft_transfers_df.show();
        System.out.println(" --------------- Data persisted into buyers_trades_count ----------------------- ");
    }

    public static void findTopSellers(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding Top Sellers from table: " + tableName);
        Dataset<Row> rowDataset = spark.read().jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableName, MORALIS_CONNECTION_MANAGER.getProps())
                .select(col("from_address").as("seller_address"))
                .groupBy(col("seller_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "sellers_trades_count", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //.limit(10);
        System.out.println(" --------------- Data persisted into sellers_trades_count ----------------------- ");

    }

}
