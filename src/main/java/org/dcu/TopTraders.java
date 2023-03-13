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

    private static String tableName = SparkDataProcessor.TRANSFER_TABLE;

    public static void findTopBuyers(SparkSession spark, ConnectionManager connectionManager) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding Top Buyers from table: " + tableName);
        Dataset<Row> rowDataset = spark.read().jdbc(connectionManager.getUrl() + SparkDataProcessor.READ_DB_SCHEMA, tableName, connectionManager.getProps())
                .select(col("to_address").as("buyer_address"))
                .groupBy(col("buyer_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write().mode(SaveMode.Overwrite)
                .jdbc(connectionManager.getUrl() + SparkDataProcessor.WRITE_DB_SCHEMA, "buyers_trades_count", connectionManager.getProps());

        //nft_transfers_df.show();
        System.out.println(" --------------- Data persisted into buyers_trades_count ----------------------- ");
    }

    public static void findTopSellers(SparkSession spark, ConnectionManager connectionManager) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding Top Sellers from table: " + tableName);
        Dataset<Row> rowDataset = spark.read().jdbc(connectionManager.getUrl() + SparkDataProcessor.READ_DB_SCHEMA, tableName, connectionManager.getProps())
                .select(col("from_address").as("seller_address"))
                .groupBy(col("seller_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write().mode(SaveMode.Overwrite)
                .jdbc(connectionManager.getUrl() + SparkDataProcessor.WRITE_DB_SCHEMA, "sellers_trades_count", connectionManager.getProps());

        //.limit(10);
        System.out.println(" --------------- Data persisted into sellers_trades_count ----------------------- ");

    }

}
