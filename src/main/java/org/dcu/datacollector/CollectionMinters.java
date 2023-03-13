package org.dcu.datacollector;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
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
public class CollectionMinters {

    public static final MoralisConnectionManager MORALIS_CONNECTION_MANAGER = new MoralisConnectionManager();
    public static final DcuSparkConnectionManager DCU_SPARK_CONNECTION_MANAGER = new DcuSparkConnectionManager();

    private static String tableName = "nft_contract_entity";

    public static void findTopMinters(SparkSession spark) {

        System.out.println(">>>> Finding Top Minters from table: " + tableName);

        spark.read()
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), tableName, DCU_SPARK_CONNECTION_MANAGER.getProps())
                .select(col("minterAddress").as("minterAddress"), col("nftAddress").as("nftAddress"),
                        col("tokenId").as("tokenId"), col("name").as("name"))
                .createOrReplaceTempView("tempView");

        //Top minters
        // Query to count the number of records for each minterAddress
        String query_top_minters = "select minterAddress, count(*) as cnt from tempView group by minterAddress order by cnt desc";
        String query_top_minted_coll = "select nftAddress, name, count(*) as cnt from tempView group by nftAddress, name order by cnt desc";

        // Execute the query and save the result
        Dataset<Row> df_1 = spark.sql(query_top_minters);
        df_1.write().mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "top_minters", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //df_1.show();
        System.out.println(" --------------- Data persisted into top_minters ----------------------- ");
/* Sample output
+--------------------+---+
|       minterAddress|cnt|
+--------------------+---+
|                null|242|
|0x232036ed540221a...|162|
|0x6bcbe6c086cc668...|120|
|0x98bd3cd04e599bc...|113|
|0x2a8f200c4a79c66...|104|
|0xf7a926e197e2a07...|102|
|0x727ac56455e8441...|100|
|0x0be0cce50e087c1...|100|
|0xa0a39727f22fae7...|100|
|0x9b353523f266b3e...|100|
|0x98849eca30be416...|100|
|0xf916719c7251e10...| 90|
|0xbc8dafeaca658ae...| 89|
|0xb154587a54aa67f...| 83|
|0x828a33a01145a34...| 83|
|0xa8eb4ee8087892b...| 72|
|0x082a7c12a2ef221...| 66|
|0xe11473d82992110...| 63|
|0xf09c81223fda440...| 57|
|0xdf7f8984b4a8105...| 56|
+--------------------+---+
only showing top 20 rows
* */


        // Execute the query and save the result
        Dataset<Row> df_2 = spark.sql(query_top_minted_coll);
        df_2.write().mode(SaveMode.Overwrite)
                .jdbc(DCU_SPARK_CONNECTION_MANAGER.getUrl(), "top_minted_collections", DCU_SPARK_CONNECTION_MANAGER.getProps());

        //df_2.show();
        System.out.println(" --------------- Data persisted into top_minted_collections ----------------------- ");
/*
* Sample output
+--------------------+--------------+----+
|          nftAddress|          name| cnt|
+--------------------+--------------+----+
|0x000E49C87d28744...|       Goobers|8687|
|0x00000633Df12288...|         wLoot|1088|
|0x000000000437b3C...|        My NFT| 139|
|0x000000F36EDb9d4...| ShibaInuSpace|  29|
|0x00000000000b7F8...|         zLoot|  29|
|0x00000000fFfD97a...|      Rare QRs|  23|
|0x000000873EE8C0b...|       Unicorn|   3|
|0x00027FFc0FbEd9a...|NFTworldArt102|   1|
|0x0000009FC3Fea00...| ShibaInuSpace|   1|
+--------------------+--------------+----+
*
* */

    }



}