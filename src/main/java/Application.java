import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {

        StructType schema=new StructType()
                .add("search", DataTypes.StringType)
                .add("region",DataTypes.StringType)
                .add("timestamp",DataTypes.TimestampType);


        SparkSession sparkSession=SparkSession.builder().master("local")
                .appName("Spark Search Analysis").getOrCreate();
        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "165.232.45.30:9092")
                .option("subscribe", "search").load();
        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");
        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema).as("jsontostructs"))
                .select("jsontostructs.*");
        Dataset<Row> searchGroup = valueDS.groupBy("search").count();
        Dataset<Row> searchResult = searchGroup.sort(functions.desc("count")).limit(5);
        searchResult.show();


    }
}
