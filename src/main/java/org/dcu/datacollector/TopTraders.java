package org.dcu.datacollector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import static org.apache.spark.sql.functions.*;
import static org.dcu.util.RandomNameGenerator.generateRandomBuyersSellersName;

/**
 * This class is responsible for running spark job to calculate the Trends for buyers and sellers.
 * It provides the deep insight on each NFT, like
 * - Total buyers
 * - Total Sellers
 * - Buyers/Sellers distribution over time.
 *
 * This can be used to get the insight and trends on NFTs.
 *
 * <p>
 *     Following tables will hold the output of this processing
 *
 *
 * 1. full_buyers_trades_count
 * 2. full_sellers_trades_count
 *
 * that can be used to create two metrics
 *
 * 1. Top buyers
 * 2. Top sellers
 */
public class TopTraders {

    public static final MoralisConnectionManager MORALIS_CONNECTION_MANAGER = new MoralisConnectionManager();
    public static final DcuSparkConnectionManager DCU_SPARK_CONNECTION_MANAGER = new DcuSparkConnectionManager();

    private static final String tableToReadData = MoralisConnectionManager.TABLE_NFT_TRANSFERS;

    private static int num_partitions = 256;

    public static void findTopBuyersSellers(SparkSession spark) {

        System.out.println(">>>> Finding Top Buyers from table>>: " + tableToReadData);

        UserDefinedFunction randomNameUDF = udf((String s) -> generateRandomBuyersSellersName(), DataTypes.StringType);

        Dataset<Row> fullDataset = spark.read()
                .jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableToReadData, MORALIS_CONNECTION_MANAGER.getProps())
                .select(col("to_address").as("buyer_address"), col("from_address").as("seller_address"))
                .withColumn("row_num", monotonically_increasing_id())
                .repartitionByRange(num_partitions, col("row_num"));

        // cache teh dataset
        fullDataset.cache();

        Dataset<Row> buyersDataset = fullDataset.groupBy(col("buyer_address"))
                .count()
                .withColumn("buyer_name", randomNameUDF.apply(col("buyer_address")))
                .orderBy(desc("count"));

        buyersDataset
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "full_buyers_trades_count", DCU_SPARK_CONNECTION_MANAGER.getProps());

        System.out.println(">>>> Finding Top Sellers from table: " + tableToReadData);

        Dataset<Row> sellersDataset = fullDataset.groupBy(col("seller_address"))
                .count()
                .withColumn("seller_name", randomNameUDF.apply(col("seller_address")))
                .orderBy(desc("count"));

        sellersDataset
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "full_sellers_trades_count", DCU_SPARK_CONNECTION_MANAGER.getProps());

        System.out.println(" --------------- Data persisted into tables ----------------------- ");
    }


    public static void findTopBuyers(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding Top Buyers from table: " + tableToReadData);
        Dataset<Row> rowDataset = spark.read()
                .jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableToReadData, MORALIS_CONNECTION_MANAGER.getProps())
                .select(col("to_address").as("buyer_address"))
                .groupBy(col("buyer_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.withColumn("row_num", monotonically_increasing_id())
                .repartitionByRange(num_partitions, col("row_num"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "buyers_trades_count", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //nft_transfers_df.show();
        System.out.println(" --------------- Data persisted into buyers_trades_count ----------------------- ");
    }

    public static void findTopSellers(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding Top Sellers from table: " + tableToReadData);
        Dataset<Row> rowDataset = spark.read().jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableToReadData, MORALIS_CONNECTION_MANAGER.getProps())
                .select(col("from_address").as("seller_address"))
                .groupBy(col("seller_address"))
                .count()
                .orderBy(desc("count"));

        rowDataset.withColumn("row_num", monotonically_increasing_id())
                .repartitionByRange(num_partitions, col("row_num"))
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "sellers_trades_count", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //.limit(10);
        System.out.println(" --------------- Data persisted into sellers_trades_count ----------------------- ");

    }

}
