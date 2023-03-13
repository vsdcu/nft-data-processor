package org.dcu.datacollector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DecimalType;
import org.dcu.database.DcuSparkConnectionManager;
import org.dcu.database.MoralisConnectionManager;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;
import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_TRANSFERS;

public class NFTValueProposition {

    public static final MoralisConnectionManager MORALIS_CONNECTION_MANAGER = new MoralisConnectionManager();
    public static final DcuSparkConnectionManager DCU_SPARK_CONNECTION_MANAGER = new DcuSparkConnectionManager();
    private static String tableName = TABLE_NFT_TRANSFERS;

    public static void findNFTsValueProposition(SparkSession spark) {

        // read from GCP MySQL database, filter and then persist back in new table
        System.out.println(">>>> Finding nft_value_propositions from table: " + tableName);
        Dataset<Row> df = spark.read()
                .jdbc(MORALIS_CONNECTION_MANAGER.getUrl(), tableName, MORALIS_CONNECTION_MANAGER.getProps());

//        Dataset<Row> firstTransfer = df.groupBy("nft_address", "token_id")
//                .agg(first("value").as("first_value"));
//        Dataset<Row> lastTransfer = df.groupBy("nft_address", "token_id")
//                .agg(last("value").as("last_value"));

        df.show();

        Dataset<Row> firstTransfer = df
                .groupBy(col("nft_address"), col("token_id"))
                .agg(min(col("block_timestamp")).as("first_timestamp"),
                        first(col("value")).cast(new DecimalType(38,0)).as("first_transfer"));

        Dataset<Row> lastTransfer = df
                .groupBy(col("nft_address"), col("token_id"))
                .agg(max(col("block_timestamp")).as("last_timestamp"),
                        last(col("value")).cast(new DecimalType(38,0)).as("last_transfer"));

        Dataset<Row> difference = firstTransfer.join(lastTransfer,
                        firstTransfer.col("nft_address").equalTo(lastTransfer.col("nft_address"))
                                .and(firstTransfer.col("token_id").equalTo(lastTransfer.col("token_id"))))
                .withColumn("difference", expr("last_transfer - first_transfer").cast(new DecimalType(38,0)))
                .select(firstTransfer.col("nft_address"), firstTransfer.col("token_id"),
                        firstTransfer.col("first_transfer"), lastTransfer.col("last_transfer"),
                        col("difference"))
                .orderBy(desc("difference"));

        difference.write()
                .mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "nft_value_propositions", DCU_SPARK_CONNECTION_MANAGER.getProps());


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

        firstTransfer.show();
        lastTransfer.show();
        difference.show();
        System.out.println(" --------------- Data persisted into nft_value_propositions ----------------------- ");
    }
}