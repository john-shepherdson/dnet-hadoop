package eu.dnetlib.dhp.tag.resolver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JoinEntityResolver implements EntityResolver{
    @Override
    public Dataset<Row> resolve(SparkSession spark, String graphDatabase, String leftEntity, String rightEntity, String relation) {
        Dataset<Row> left = spark.table(graphDatabase + "/" + leftEntity);
        Dataset<Row> rigth = spark.table(graphDatabase + "/" + rightEntity);
        Dataset<Row> rels = spark.table(graphDatabase + "/relation")
                .where("relClass = '" + relation + "'");
        return left.join(rels, left.col("id").equalTo(rels.col("source")))
                .join(rigth, rigth.col("id").equalTo(rels.col("target")));

    }
}
