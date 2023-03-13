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
 * This will persist output in one table
 * <p>
 * 1. trades_by_collection
 * <p>
 * that can be used to create two metrics
 * <p>
 * 1. Top traded NFT collections
 * 2. Least traded NFT collections
 */
public class CollectionTrades {

    public static final MoralisConnectionManager MORALIS_CONNECTION_MANAGER = new MoralisConnectionManager();
    public static final DcuSparkConnectionManager DCU_SPARK_CONNECTION_MANAGER = new DcuSparkConnectionManager();

    private static String tableName = TABLE_NFT_TRANSFERS;

    public static void findTotalTradesByNFTCollection(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding MostTradedNFTCollection from table: " + tableName);

        Dataset<Row> rowDataset = spark.read()
                .jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableName, MORALIS_CONNECTION_MANAGER.getProps())
                .select("nft_address")
                .groupBy(col("nft_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "trades_by_collection", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //nft_transfers_df.show();
        System.out.println(" --------------- Data persisted into trades_by_collection ----------------------- ");
    }


    /**
     * Find total number of trades for each token-id present in an NFT collection.
     *
     * @param spark
     */
    public static void findTotalTradesByTokenIdInNFTCollection(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding findTotalTradesByTokenIdInNFTCollection from table: " + tableName);

        Dataset<Row> rowDataset = spark.read().jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableName, MORALIS_CONNECTION_MANAGER.getProps())
                .select("nft_address", "token_id")
                .groupBy(col("nft_address"), col("token_id"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "token_trades_by_collection", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //nft_transfers_df.show();
        System.out.println(" --------------- Data persisted into token_trades_by_collection ----------------------- ");
    }


}
