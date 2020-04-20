package eu.dnetlib.dhp.common;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.ThrowingConsumer;
import java.util.Objects;
import java.util.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/** SparkSession utility methods. */
public class SparkSessionSupport {

    private SparkSessionSupport() {}

    /**
     * Runs a given function using SparkSession created using default builder and supplied
     * SparkConf. Stops SparkSession when SparkSession is managed. Allows to reuse SparkSession
     * created externally.
     *
     * @param conf SparkConf instance
     * @param isSparkSessionManaged When true will stop SparkSession
     * @param fn Consumer to be applied to constructed SparkSession
     */
    public static void runWithSparkSession(
            SparkConf conf,
            Boolean isSparkSessionManaged,
            ThrowingConsumer<SparkSession, Exception> fn) {
        runWithSparkSession(
                c -> SparkSession.builder().config(c).getOrCreate(),
                conf,
                isSparkSessionManaged,
                fn);
    }

    /**
     * Runs a given function using SparkSession created with hive support and using default builder
     * and supplied SparkConf. Stops SparkSession when SparkSession is managed. Allows to reuse
     * SparkSession created externally.
     *
     * @param conf SparkConf instance
     * @param isSparkSessionManaged When true will stop SparkSession
     * @param fn Consumer to be applied to constructed SparkSession
     */
    public static void runWithSparkHiveSession(
            SparkConf conf,
            Boolean isSparkSessionManaged,
            ThrowingConsumer<SparkSession, Exception> fn) {
        runWithSparkSession(
                c -> SparkSession.builder().config(c).enableHiveSupport().getOrCreate(),
                conf,
                isSparkSessionManaged,
                fn);
    }

    /**
     * Runs a given function using SparkSession created using supplied builder and supplied
     * SparkConf. Stops SparkSession when SparkSession is managed. Allows to reuse SparkSession
     * created externally.
     *
     * @param sparkSessionBuilder Builder of SparkSession
     * @param conf SparkConf instance
     * @param isSparkSessionManaged When true will stop SparkSession
     * @param fn Consumer to be applied to constructed SparkSession
     */
    public static void runWithSparkSession(
            Function<SparkConf, SparkSession> sparkSessionBuilder,
            SparkConf conf,
            Boolean isSparkSessionManaged,
            ThrowingConsumer<SparkSession, Exception> fn) {
        SparkSession spark = null;
        try {
            spark = sparkSessionBuilder.apply(conf);
            fn.accept(spark);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (Objects.nonNull(spark) && isSparkSessionManaged) {
                spark.stop();
            }
        }
    }
}
