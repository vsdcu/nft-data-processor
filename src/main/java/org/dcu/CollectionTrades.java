package org.dcu;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.ConnectionManager;
import org.dcu.processor.SparkDataProcessor;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;


/**
 * This will persist output in one table
 *
 * 1. trades_by_collection
 *
 * that can be used to create two metrics
 *
 * 1. Top traded NFT collections
 * 2. Least traded NFT collections
 */
public class CollectionTrades {

    private static String tableName = SparkDataProcessor.TRANSFER_TABLE;

    public static void findTotalTradesByNFTCollection(SparkSession spark, ConnectionManager connectionManager) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding MostTradedNFTCollection from table: " + tableName);

        Dataset<Row> rowDataset = spark.read().jdbc(connectionManager.getUrl() + SparkDataProcessor.READ_DB_SCHEMA, tableName, connectionManager.getProps())
                .select("nft_address")
                .groupBy(col("nft_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write().mode(SaveMode.Overwrite)
                .jdbc(connectionManager.getUrl() + SparkDataProcessor.WRITE_DB_SCHEMA, "trades_by_collection", connectionManager.getProps());

        //nft_transfers_df.show();
        System.out.println(" --------------- Data persisted into trades_by_collection ----------------------- ");
    }


    /**
     * Find total number of trades for each token-id present in an NFT collection.
     *
     * @param spark
     * @param connectionManager
     */
    public static void findTotalTradesByTokenIdInNFTCollection(SparkSession spark, ConnectionManager connectionManager) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding findTotalTradesByTokenIdInNFTCollection from table: " + tableName);

        Dataset<Row> rowDataset = spark.read().jdbc(connectionManager.getUrl() + SparkDataProcessor.READ_DB_SCHEMA, tableName, connectionManager.getProps())
                .select("nft_address", "token_id")
                .groupBy(col("nft_address"), col("token_id"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write().mode(SaveMode.Overwrite)
                .jdbc(connectionManager.getUrl() + SparkDataProcessor.WRITE_DB_SCHEMA, "token_trades_by_collection", connectionManager.getProps());

        //nft_transfers_df.show();
        System.out.println(" --------------- Data persisted into token_trades_by_collection ----------------------- ");
    }



}
