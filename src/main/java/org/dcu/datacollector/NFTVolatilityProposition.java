package org.dcu.datacollector;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DecimalType;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import static org.apache.spark.sql.functions.*;

public class NFTVolatilityProposition {

    public static final MoralisConnectionManager MORALIS_CONNECTION_MANAGER = new MoralisConnectionManager();
    public static final DcuSparkConnectionManager DCU_SPARK_CONNECTION_MANAGER = new DcuSparkConnectionManager();

    //private static final String tableToReadData = DcuSparkConnectionManager.TABLE_NFT_VALUE_PROPOSITION;

    private static final String tableToReadData = MoralisConnectionManager.TABLE_NFT_TRANSFERS;

    private static final String tableCollectionInfo = DcuSparkConnectionManager.TABLE_NFT_COLLECTION_INFO;

    private static int num_partitions = 128;

    public static void findNFTsVolatility(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding nft_volatility from table: " + tableToReadData);

//        String combinedTablesData = "(SELECT nft_transfers.*, nft_collection_info.name, symbol FROM nft_transfers JOIN " +
//                "nft_collection_info ON nft_transfers.nft_address = nft_collection_info.nft_address) as combined_data";

        Dataset<Row> nft_transfer_df = spark.read()
                .jdbc(MORALIS_CONNECTION_MANAGER.getUrl(),
                        tableToReadData,
                        MORALIS_CONNECTION_MANAGER.getProps())
                .withColumn("nftAddressHash", hash(col("nft_address")))
                .repartition(num_partitions, col("nftAddressHash"))
                .cache();
        //df.show();

        Dataset<Row> nft_collection_info_df = spark.read()
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(),
                        tableCollectionInfo,
                        DCU_SPARK_CONNECTION_MANAGER.getProps())
                .withColumn("nftAddressHash", hash(col("nft_address")))
                .repartition(num_partitions, col("nftAddressHash")).cache();

        Dataset<Row> combined_df = nft_transfer_df.join(nft_collection_info_df.as("collection"),
                        nft_transfer_df.col("nft_address").equalTo(col("collection.nft_address")))
                .select(col("collection.nft_address").as("collection_address"),
                        col("name").as("collection_name"),
                        col("symbol").as("collection_symbol"),
                        col("value").as("collection_value")).cache();
        //combined_df.show();

        nft_transfer_df.unpersist();
        nft_collection_info_df.unpersist();

        //NFT collection volatility
// First, calculate the avg price of each nft collection
       Dataset<Row> avgPrice_df = combined_df
                .groupBy(col("collection_address"), col("collection_name"), col("collection_symbol"))
                .agg(avg(col("collection_value").cast(new DecimalType(38, 0))).as("avg_price"))
                .select("collection_address", "collection_name", "collection_symbol", "avg_price")
                .orderBy(desc("avg_price"));
        System.out.println("----avgPrice_df------");
        //avgPrice_df.show(20);
        avgPrice_df
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "full_nft_collections_avg_price", DCU_SPARK_CONNECTION_MANAGER.getProps());



        Dataset<Row> meanPrice_df = combined_df
                .groupBy(col("collection_address"), col("collection_name"), col("collection_symbol"))
                .agg(mean(col("collection_value").cast(new DecimalType(38, 0))).alias("mean_price"))
                .select("collection_address", "collection_name", "collection_symbol", "mean_price")
                .orderBy(desc("mean_price"));
        System.out.println("----meanPrice_df------");
        //meanPrice_df.show(20);
        meanPrice_df
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "full_nft_collections_mean_price", DCU_SPARK_CONNECTION_MANAGER.getProps());



 // Calculate the variance of the prices for each NFT
        Dataset<Row> variance_df = combined_df
                .groupBy(col("collection_address"), col("collection_name"), col("collection_symbol"), col("collection_value"))
                .agg(
                functions.avg("collection_value").alias("avg_price"),
                functions.pow(functions.col("collection_value").minus(functions.avg("collection_value")), 2).alias("price_diff")
        ).groupBy(col("collection_address"), col("collection_name"), col("collection_symbol"))
                .agg(
                functions.sum("price_diff").alias("sum_price_diff"),
                functions.count("price_diff").alias("count_price_diff")
        ).withColumn("variance", functions.col("sum_price_diff").divide(functions.col("count_price_diff")))
                .select("collection_address", "collection_name", "collection_symbol", "variance")
                .orderBy(desc("variance"));

        System.out.println("----variance_df------");
        //variance_df.show();
        variance_df
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "full_nft_collections_variance_price", DCU_SPARK_CONNECTION_MANAGER.getProps());


//        joined_df.select("nft_address", (expr("price - mean_price").cast(new DecimalType(38, 0)) ** 2).alias("price_diff"))
//                      .groupBy("nft_id") \
//                      .agg({"price_diff": "mean"}) \
//                      .withColumnRenamed("avg(price_diff)", "price_variance")
//

//        Dataset<Row> lastTransfer = df.groupBy(col("nft_address"), col("token_id"))
//                .agg(max(col("block_timestamp")).as("last_timestamp"),
//                        last(col("value")).cast(new DecimalType(38, 0)).as("last_transfer"));
//
//        // uncache teh df to release memory
//        df.unpersist();
//
//// Then, join the two datasets and calculate the difference
//        Dataset<Row> difference = firstTransfer
//                .join(lastTransfer, firstTransfer.col("nft_address").equalTo(lastTransfer.col("nft_address"))
//                        .and(firstTransfer.col("token_id").equalTo(lastTransfer.col("token_id"))))
//                .withColumn("difference", expr("last_transfer - first_transfer").cast(new DecimalType(38, 0)))
//                .select(firstTransfer.col("nft_address"),
//                        firstTransfer.col("token_id"),
//                        firstTransfer.col("first_transfer"),
//                        lastTransfer.col("last_transfer"), col("difference"));
//
//// cache the dataset to avoid expensive re-computation
//        difference.cache();

// Finally, dumping the data to the database
//        difference
//                .repartition(num_partitions, col("nft_address"), col("token_id"))
//                .write()
//                .mode(SaveMode.Overwrite)
//                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "c_full_nft_value_propositions", DCU_SPARK_CONNECTION_MANAGER.getProps());

/* Sample output of the above logic
+--------------------+-----------+-------------------+--------------------+--------------------+
|         nft_address|   token_id|     first_transfer|       last_transfer|          difference|
+--------------------+-----------+-------------------+--------------------+--------------------+
|0x02509651f2cfB46...|         40|  99000000000000000|20970160697985314000|20871160697985314000|
|0x0062b396597FE83...|          0|1000000000000000000|16000000000000000000|15000000000000000000|
|0x059EDD72Cd353dF...|       9185|                  0|12800000000000000000|12800000000000000000|
|0x059EDD72Cd353dF...|       5826|                  0|12250000000000000000|12250000000000000000|
|0x04fDfcA42b88Bda...|      80265|5000000000000000000|15000000000000000000|10000000000000000000|
|0x0427743DF720801...|         91|                  0| 7549999999999999200| 7549999999999999200|
|0x0427743DF720801...|        311|                  0| 7500000000000000900| 7500000000000000900|
|0x0427743DF720801...|        315|                  0| 4974999999999999200| 4974999999999999200|
|0x0427743DF720801...|        222|                  0| 4974999999999999200| 4974999999999999200|
|0x053afa4D216F20D...|          1|2088700000000000000| 5959200000000000000| 3870500000000000000|
|0x02509651f2cfB46...|          7|                  0| 2000000000000000000| 2000000000000000000|
|0x062E691c2054dE8...|       2269|                  0| 1990000000000000000| 1990000000000000000|
|0x062E691c2054dE8...|         83|                  0| 1990000000000000000| 1990000000000000000|
|0x053afa4D216F20D...|          0|5000000000000000000| 6700000000000000000| 1700000000000000000|
|0x02E97aAec3551dC...|45000040050|                  0| 1380000000000000000| 1380000000000000000|
|0x062E691c2054dE8...|       2154|                  0| 1313000000000000000| 1313000000000000000|
|0x0029df988673ab4...|         31|                  0| 1110000000000000000| 1110000000000000000|
|0x0029df988673ab4...|         26|                  0| 1000000000000000000| 1000000000000000000|
|0x03d736D07155c48...|          5|                  0| 1000000000000000000| 1000000000000000000|
|0x0029df988673ab4...|         29|                  0| 1000000000000000000| 1000000000000000000|
+--------------------+-----------+-------------------+--------------------+--------------------+
only showing top 20 rows
*/

        //firstTransfer.show();
        //lastTransfer.show();
        //difference.show();
        System.out.println(" --------------- Data persisted into tables ----------------------- ");
    }
}