package eu.dnetlib.dhp.oa.graph.dump.graph;

import eu.dnetlib.dhp.oa.graph.dump.Utils;
import eu.dnetlib.dhp.oa.graph.dump.community.CommunityMap;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class DumpOrganization implements Serializable {

    public void run(Boolean isSparkSessionManaged, String inputPath, String outputPath ) {

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    execDump(spark, inputPath, outputPath);

                });
    }

    private void execDump(SparkSession spark, String inputPath, String outputPath) {

        Utils.readPath(spark, inputPath, Organization.class)
                .map(org -> OrganizationMapper.map(org), Encoders.bean(eu.dnetlib.dhp.schema.dump.oaf.graph.Organization.class))
                .write()
                .option("compression","gzip")
                .mode(SaveMode.Overwrite)
                .json(outputPath);


    }


}
