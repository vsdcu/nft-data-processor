package org.dcu.processor;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.dcu.CollectionTrades;
import org.dcu.TopTraders;
import org.dcu.database.MoralisConnectionManager;

import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_CONTRACTS;
import static org.dcu.database.MoralisConnectionManager.TABLE_NFT_TRANSFERS;

public class SparkDataProcessor {

    public static void main(String[] args) {

        String SPARK_HOME = "/Users/vinit/spark_home/spark-3.3.2-bin-hadoop3";

        //spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("NFT-Data_Processor")
                .set("spark.app.id", "spark-nft-processor");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        System.out.println(">>>> Spark session : " + spark);

        // read from GCP MySQL database
//        String tableName = TABLE_NFT_CONTRACTS;
//        System.out.println(">>>> Table name : " + tableName);
//        MoralisConnectionManager moralisConnectionManager = new MoralisConnectionManager();
//        Dataset<Row> nft_contracts = spark.read().jdbc(moralisConnectionManager.getUrl(), tableName, moralisConnectionManager.getProps()).select("json_data");
//        nft_contracts.show();

        // read from GCP MySQL database
        //String tableName = "krys_nft_contracts";
        //System.out.println(">>>> Table name : " + tableName);

        //Dataset<Row> nft_contracts = spark.read().jdbc(connectionManager.getUrl(), tableName, connectionManager.getProps()).select("json_data");
        //nft_contracts.show();
        //count data form nft_contracts
        //long count = nft_contracts.count();
        //System.out.println(">>>>>>>>>>>>>>>>>>>>>.Total records in table {"+tableName+"} : " + count);

        //find top buyers
        TopTraders.findTopBuyers(spark);

        //find top sellers
        //TopTraders.findTopSellers(spark);

        //find total trades for each collection
        //CollectionTrades.findTotalTradesByNFTCollection(spark);

        //find total trades for each token-id present in collection
        //CollectionTrades.findTotalTradesByTokenIdInNFTCollection(spark);

        spark.stop();

    }






}
