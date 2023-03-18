package org.dcu.datacollector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;


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

    //private static final String tableToReadData = MoralisConnectionManager.MRTC_NFT_TRANSFERS;

    private static final String tableToReadData = MoralisConnectionManager.TABLE_NFT_TRANSFERS;

    public static final int NUM_PARTITIONS = 16014;

    public static void findTrends(SparkSession spark) {
        System.out.println("Loading data in dataframe from table: " + tableToReadData);
        //load data in dataframe
        Dataset<Row> rowDataset = spark.read().jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableToReadData, MORALIS_CONNECTION_MANAGER.getProps())
                .select("nft_address", "token_id")
                .withColumn("row_num", monotonically_increasing_id());;

        rowDataset.repartitionByRange(NUM_PARTITIONS, col("row_num")).cache();

        //group_by collection
        Dataset<Row> collections_df = rowDataset
                .select(col("nft_address"))
                .groupBy(col("nft_address"))
                .count();

        //dumping the processed data into spark-db
        collections_df.cache();
        collections_df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "full_trades_by_collection", DCU_SPARK_CONNECTION_MANAGER.getProps());

        System.out.println(" --------------- Data persisted into trades_by_collection ----------------------- ");

        // part-2
        Dataset<Row> tokens_df = rowDataset
                .select("nft_address", "token_id")
                .groupBy(col("nft_address"), col("token_id"))
                .count();

        tokens_df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "full_token_trades_by_collection", DCU_SPARK_CONNECTION_MANAGER.getProps());

        System.out.println(" --------------- Data persisted into token_trades_by_collection ----------------------- ");
    }

    private static void insertDataInBatches1(Dataset<Row> myDataset) {
        myDataset.foreachPartition(rows -> {
            Connection conn = DriverManager.getConnection(DCU_SPARK_CONNECTION_MANAGER.getUrl(), DCU_SPARK_CONNECTION_MANAGER.getProps());
            //conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO mrtc_trades_by_collection (nft_address, count) VALUES (?, ?)");
            while (rows.hasNext()) {
                Row row = rows.next();
                pstmt.setString(1, row.getString(0));
                pstmt.setLong(2, row.getLong(1));
                pstmt.addBatch();
            }
            long[] rowsInserted = pstmt.executeLargeBatch();
            //conn.commit();
            System.out.println("\n\n******* trades_by_collection Rows affected: " + Arrays.toString(rowsInserted));
            pstmt.close();
            conn.close();
        });
    }


    private static void insertDataInBatches2(Dataset<Row> myDataset) {
        myDataset.explain();

        myDataset.repartitionByRange(NUM_PARTITIONS, col("row_num")).foreachPartition(rows -> {
            Connection conn = DriverManager.getConnection(DCU_SPARK_CONNECTION_MANAGER.getUrl(), DCU_SPARK_CONNECTION_MANAGER.getProps());
            //conn.setAutoCommit(false);
            PreparedStatement pstmt = conn.prepareStatement("INSERT INTO mrtc_token_trades_by_collection (nft_address, token_id, count) VALUES (?, ?, ?)");
            while (rows.hasNext()) {
                Row row = rows.next();
                pstmt.setString(1, row.getString(0));
                pstmt.setString(2, row.getString(1));
                pstmt.setLong(3, row.getLong(2));
                pstmt.addBatch();
            }
            long[] rowsInserted = pstmt.executeLargeBatch();
            //conn.commit();
            System.out.println("\n\n******* token_trades_by_collection Rows affected: " + Arrays.toString(rowsInserted));
            pstmt.close();
            conn.close();
        });
    }

    public static void findTotalTradesByNFTCollection(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        Dataset<Row> rowDataset = spark.read()
                .jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableToReadData, MORALIS_CONNECTION_MANAGER.getProps())
                .select("nft_address")
                .groupBy(col("nft_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "trades_by_collection", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //nft_transfers_df.show();
    }


    /**
     * Find total number of trades for each token-id present in an NFT collection.
     *
     * @param spark
     */
    public static void findTotalTradesByTokenIdInNFTCollection(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding findTotalTradesByTokenIdInNFTCollection from table: " + tableToReadData);

        Dataset<Row> rowDataset = spark.read().jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableToReadData, MORALIS_CONNECTION_MANAGER.getProps())
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
