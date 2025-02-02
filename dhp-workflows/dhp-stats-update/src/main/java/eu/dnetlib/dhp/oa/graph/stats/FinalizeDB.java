package eu.dnetlib.dhp.oa.graph.stats;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class FinalizeDB {

    private static final Logger log = LoggerFactory.getLogger(FinalizeDB.class);

    private final ArgumentApplicationParser parser;

    public FinalizeDB(ArgumentApplicationParser parser) {
        this.parser = parser;
    }


    public static void main(String[] args) throws Exception {

        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                params.put(args[i].substring(2), args[++i]);
            }
        }

        String sourceDb = params.get("stats_db_name");
        String shadowDb = params.get("stats_db_shadow_name");

        Boolean isSparkSessionManaged = Optional
                .ofNullable(params.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", params.get("hiveMetastoreUris"));

        String sql = String.format(
                "DROP DATABASE IF EXISTS %s CASCADE;/*EOS*/" +
                "CREATE DATABASE %s;", shadowDb, shadowDb);

        runWithSparkHiveSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    for (String statement : sql.split(";\\s*/\\*\\s*EOS\\s*\\*/\\s*")) {
                        log.info("executing: {}", statement);
                        long startTime = System.currentTimeMillis();
                        try {
                            spark.sql(statement).show();
                        } catch (Exception e) {
                            log.error("Error executing statement: {}", statement, e);
                            System.err.println("Error executing statement: " + statement + "\n" + e);
                            throw e;
                        }
                        log.info(
                            "executed in {}",
                            DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.S"));
                    }
                });

        runWithSparkHiveSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    spark.sql("SHOW TABLES IN " + sourceDb).collectAsList().forEach(row -> {
                        String tableName = row.getString(1);
                        String statement = String.format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s", shadowDb, tableName, sourceDb, tableName);

                        executeStatement(spark, statement);
                    });
                });
    }

    private static void executeStatement(SparkSession spark, String statement) {
        log.info("executing: {}", statement);
        long startTime = System.currentTimeMillis();
        try {
            spark.sql(statement).show();
        } catch (Exception e) {
            log.error("Error executing statement: {}", statement, e);
            System.err.println("Error executing statement: " + statement + "\n" + e);
            throw e;
        }
        log.info(
            "executed in {}",
            DurationFormatUtils.formatDuration(System.currentTimeMillis() - startTime, "HH:mm:ss.S"));
    }
}