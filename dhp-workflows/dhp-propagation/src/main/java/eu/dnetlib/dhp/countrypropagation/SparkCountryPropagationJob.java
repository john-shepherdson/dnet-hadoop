package eu.dnetlib.dhp.countrypropagation;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.PropagationConstant;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import net.sf.saxon.expr.ContextMappingFunction;
import net.sf.saxon.expr.flwor.Tuple;
import net.sf.saxon.om.Item;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

import javax.sql.DataSource;
import java.beans.Encoder;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkCountryPropagationJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCountryPropagationJob.class.getResourceAsStream("/eu/dnetlib/dhp/countrypropagation/input_countrypropagation_parameters.json")));
        parser.parseArgument(args);
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkCountryPropagationJob.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();


        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/countrytoresultfrominstitutionalrepositories";

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        List<String> whitelist = Arrays.asList(parser.get("whitelist").split(";"));
        List<String> allowedtypes = Arrays.asList(parser.get("allowedtypes").split(";"));


        datasource(spark, whitelist, outputPath, inputPath);

        //rdd(spark,whitelist,allowedtypes, outputPath, inputPath);

    }

    private static void datasource(SparkSession spark, List<String> whitelist, String outputPath, String inputPath){
        String whitelisted = "";
        for (String i : whitelist){
            whitelisted += " OR id = '" + i + "'";
        }

        String query = "SELECT source ds, target org, country.classid country " +
                       "FROM ( SELECT id " +
                               "FROM openaire.datasource " +
                               "WHERE datasourcetype.classid = 'pubsrepository::institutional' " +
                               "AND (datainfo.deletedbyinference = false " + whitelisted + ") ) d " +
                       "JOIN ( SELECT source, target " +
                               "FROM openaire.relation " +
                               "WHERE relclass = 'provides' " +
                               "AND datainfo.deletedbyinference = false ) rel " +
                       "ON d.id = rel.source " +
                       "JOIN (SELECT id, country " +
                              "FROM openaire.organization " +
                              "WHERE datainfo.deletedbyinference = false ) o " +
                       "ON o.id = rel.target";

        Dataset<Row> rels = spark.sql(query);
        rels.createOrReplaceTempView("rels");
        
        

        final JavaRDD<Row> toupdateresultsoftware = propagateOnResult(spark, "openaire.software");
        createUpdateForSoftwareDataset(toupdateresultsoftware, inputPath, spark)
                .map(s -> new ObjectMapper().writeValueAsString(s))
                .saveAsTextFile(outputPath + "/software");
        createUpdateForResultDatasetWrite(toupdateresultsoftware, outputPath, "update_software");

        JavaRDD<Row> toupdateresultdataset = propagateOnResult(spark, "openaire.dataset");
        createUpdateForDatasetDataset(toupdateresultdataset,inputPath,spark)
                .map(d -> new ObjectMapper().writeValueAsString(d))
                .saveAsTextFile(outputPath + "/dataset");
        createUpdateForResultDatasetWrite(toupdateresultdataset, outputPath, "update_dataset");

        JavaRDD<Row> toupdateresultother = propagateOnResult(spark, "openaire.otherresearchproduct");
        createUpdateForOtherDataset(toupdateresultother, inputPath, spark)
                .map(o -> new ObjectMapper().writeValueAsString(o))
                .saveAsTextFile(outputPath + "/otherresearchproduct");
        createUpdateForResultDatasetWrite(toupdateresultother, outputPath, "update_other");

        createUpdateForPublicationDataset(propagateOnResult(spark, "openaire.publication"), inputPath, spark)
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/publication");

    }

    private static JavaRDD<OtherResearchProduct> createUpdateForOtherDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/otherresearchproduct")
                .map(item -> new ObjectMapper().readValue(item, OtherResearchProduct.class))
                .mapToPair(s -> new Tuple2<>(s.getId(), s)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    OtherResearchProduct oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<Publication> createUpdateForPublicationDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/publication")
                .map(item -> new ObjectMapper().readValue(item, Publication.class))
                .mapToPair(s -> new Tuple2<>(s.getId(), s)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    Publication oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<Software> createUpdateForSoftwareDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/software")
                .map(item -> new ObjectMapper().readValue(item, Software.class))
                .mapToPair(s -> new Tuple2<>(s.getId(), s)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    Software oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> createUpdateForDatasetDataset(JavaRDD<Row> toupdateresult, String inputPath, SparkSession spark) {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        return sc.textFile(inputPath + "/dataset")
                .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Dataset.class))
                .mapToPair(d -> new Tuple2<>(d.getId(), d)).leftOuterJoin(getStringResultJavaPairRDD(toupdateresult))
                .map(c -> {
                    eu.dnetlib.dhp.schema.oaf.Dataset oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        Result r = c._2()._2().get();
                        for (Country country : r.getCountry()) {
                            if (!countries.contains(country.getClassid())) {
                                countryList.add(country);
                            }
                        }
                        oaf.setCountry(countryList);
                    }
                    return oaf;
                });
    }

    private static JavaRDD<Row> propagateOnResult(SparkSession spark, String table) {
        String query;
        query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb " +
                "FROM ( SELECT id, instance " +
                       "FROM " + table +
                       " WHERE datainfo.deletedbyinference = false)  ds " +
                       "LATERAL VIEW EXPLODE(instance) i AS inst";
        Dataset<Row> cfhb = spark.sql(query);
        cfhb.createOrReplaceTempView("cfhb");

        return countryPropagationAssoc(spark, "cfhb").toJavaRDD();

    }

    private static Dataset<Row> countryPropagationAssoc(SparkSession spark, String cfhbTable){
        String  query = "SELECT id, collect_set(country) country "+
                "FROM ( SELECT id, country " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON cf = ds     " +
                "UNION ALL " +
                "SELECT id , country     " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON hb = ds ) tmp " +
                "GROUP BY id";
        return spark.sql(query);
    }

    private static JavaPairRDD<String, Result> getStringResultJavaPairRDD(JavaRDD<Row> toupdateresult) {
        return toupdateresult.map(c -> {
            List<Country> countryList = new ArrayList<>();
            List<String> tmp = c.getList(1);
            for (String country : tmp) {
                countryList.add(getCountry(country));
            }
            Result r = new Result();
            r.setId(c.getString(0));
            r.setCountry(countryList);
            return r;
        }).mapToPair(r -> new Tuple2<>(r.getId(), r));
    }

    private static void createUpdateForResultDatasetWrite(JavaRDD<Row> toupdateresult, String outputPath, String type){
        toupdateresult.map(c -> {
            List<Country> countryList = new ArrayList<>();
            List<String> tmp = c.getList(1);
            for (String country : tmp) {
                countryList.add(getCountry(country));
            }
            Result r = new Result();
            r.setId(c.getString(0));
            r.setCountry(countryList);
            return r;

        }).map(r ->new ObjectMapper().writeValueAsString(r))
                .saveAsTextFile(outputPath+"/"+type);
    }

    private static Dataset<Row> countryPropagation(SparkSession spark, String cfhbTable){
       String  query = "SELECT id, collect_set(named_struct('classid', country, 'classname', country, " +
                "'datainfo', named_struct( 'deletedbyinference', false, " +
                "'inferenceprovenance','" + PROPAGATION_DATA_INFO_TYPE +"'," +
                "'inferred',true,'invisible',false, " +
                "'provenanceaction', named_struct('classid','" + PROPAGATION_COUNTRY_INSTREPO_CLASS_ID + "'," +
                "'classname','" + PROPAGATION_COUNTRY_INSTREPO_CLASS_NAME +"'," +
                "'schemeid','" + DNET_SCHEMA_ID +"'," +
                "'schemename','" + DNET_SCHEMA_NAME +"')  , " +
                "'trust','0.9') ,'schemeid','" + DNET_COUNTRY_SCHEMA +"','schemename','" + DNET_COUNTRY_SCHEMA + "')) country  " +
                "FROM ( SELECT id, country " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON cf = ds     " +
                "UNION ALL " +
                "SELECT id , country     " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON hb = ds ) tmp " +
                "GROUP BY id";
        return spark.sql(query);
    }

    private static void rdd(SparkSession spark,  List<String> whitelist, List<String> allowedtypes, String outputPath, String inputPath) throws IOException {
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            JavaPairRDD<String, TypedRow> organizations = sc.textFile(inputPath + "/organization")
                .map(item -> new ObjectMapper().readValue(item, Organization.class))
                .filter(org -> !org.getDataInfo().getDeletedbyinference())
                .map(org -> {
                    TypedRow tr = new TypedRow();
                    tr.setSourceId(org.getId());
                    tr.setValue(org.getCountry().getClassid());
                    return tr;
                })
                .mapToPair(toPair());

        JavaPairRDD<String, TypedRow> organization_datasource =
                sc.textFile(inputPath + "/relation")
                        .map(item -> new ObjectMapper().readValue(item, Relation.class))
                        .filter(r -> !r.getDataInfo().getDeletedbyinference())
                        .filter(r -> RELATION_DATASOURCEORGANIZATION_REL_TYPE.equals(r.getRelType()) &&
                                RELATION_ORGANIZATION_DATASOURCE_REL_CLASS.equals(r.getRelClass()))
                        .map(r -> {
                            TypedRow tp = new TypedRow(); tp.setSourceId(r.getSource()); tp.setTargetId(r.getTarget());
                            return tp;
                        })
                        .mapToPair(toPair()); //id is the organization identifier

        JavaPairRDD<String, TypedRow> datasources = sc.textFile(inputPath + "/datasource")
                .map(item -> new ObjectMapper().readValue(item, Datasource.class))
                .filter(ds -> whitelist.contains(ds.getId()) || allowedtypes.contains(ds.getDatasourcetype().getClassid()))
                .map(ds -> new TypedRow().setSourceId(ds.getId()))
                .mapToPair(toPair());

        JavaPairRDD<String, TypedRow> datasource_country = organizations.join(organization_datasource)
                .map(x -> x._2()._1().setSourceId(x._2()._2().getTargetId())) // (OrganizationId,(TypedRow for Organization, TypedRow for Relation)
                .mapToPair(toPair()); //(DatasourceId, TypedRowforOrganziation)


        JavaPairRDD<String, TypedRow> alloweddatasources_country = datasources.join(datasource_country)
                .mapToPair(ds -> new Tuple2<>(ds._1(), ds._2()._2())).cache();

//        System.out.println("OUTPUT  *** ORGANIZATION COUNT *** " + organizations.count());
//        System.out.println("OUTPUT  *** ORGANIZATION DATASOURCE RELATIONS COUNT *** " + organization_datasource.count());
//        System.out.println("OUTPUT  ***  DATASOURCE COUNT *** " + datasources.count());
//        System.out.println("OUTPUT  ***  ALLOWED_DATASOURCE-COUNTRY COUNT *** " + alloweddatasources_country.count());

//        alloweddatasources_country.map(p -> new ObjectMapper().writeValueAsString(p))
//                .saveAsTextFile(outputPath+"/datasource_country");

       JavaRDD<Software> software = sc.textFile(inputPath + "/software")
                .map(item -> new ObjectMapper().readValue(item, Software.class));

        JavaPairRDD<String, TypedRow> datasource_software = software
                .map(oaf -> getTypedRowsDatasourceResult(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                });

        datasource_software.map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/datasource_software");

        JavaPairRDD<String, Result> sfw = software.mapToPair(p -> new Tuple2<>(p.getId(),p));

        JavaPairRDD<String,TypedRow> toupdateresult = alloweddatasources_country.join(datasource_software)
                .map(u -> {
                    TypedRow tp = u._2()._2();
                    tp.setValue(u._2()._1().getValue());
                    return tp;
                })
                .mapToPair(toPair())
                .reduceByKey((a, p) -> {
                    if (a == null) {
                        return p;
                    }
                    if (p == null) {
                        return a;
                    }
                    a.addAll(p.getAccumulator());
                    return a;
                });
        toupdateresult.map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/toupdateresult");
        //updateResult(sfw, toupdateresult, outputPath, "software");
       // createUpdateForResult(toupdateresult, outputPath, "software");



     /*  JavaRDD<Publication> publications = sc.textFile(inputPath + "/publication")
                .map(item -> new ObjectMapper().readValue(item, Publication.class));

        JavaPairRDD<String, TypedRow> datasource_publication = publications
                .map(oaf -> getTypedRowsDatasourceResult(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                });

        JavaPairRDD<String,TypedRow> toupdateresult = alloweddatasources_country.join(datasource_publication)
                .map(u -> u._2()._2().setValue(u._2()._1().getValue()))
                .mapToPair(toPair())
                .reduceByKey((a, p) -> {
                    if (a == null) {
                        return p;
                    }
                    if (p == null) {
                        return a;
                    }
                    a.addAll(p.getAccumulator());
                    return a;
                });








        JavaRDD<Publication> publications = sc.textFile(inputPath + "/publication")
                .map(item -> new ObjectMapper().readValue(item, Publication.class));
        JavaRDD<Dataset> datasets = sc.textFile(inputPath + "/dataset")
                .map(item -> new ObjectMapper().readValue(item, Dataset.class));
        JavaRDD<Software> software = sc.textFile(inputPath + "/software")
                .map(item -> new ObjectMapper().readValue(item, Software.class));
        JavaRDD<OtherResearchProduct> other = sc.textFile(inputPath + "/otherresearchproduct")
                .map(item -> new ObjectMapper().readValue(item, OtherResearchProduct.class));




        JavaPairRDD<String, TypedRow> datasource_results = publications
                .map(oaf -> getTypedRowsDatasourceResult(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                })
                .union(datasets
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }))
                .union(software
                .map(oaf -> getTypedRowsDatasourceResult(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                }))
                .union(other
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }));





        JavaPairRDD<String,TypedRow> toupdateresult = alloweddatasources_country.join(datasource_results)
                .map(u -> u._2()._2().setValue(u._2()._1().getValue()))
                .mapToPair(toPair())
                .reduceByKey((a, p) -> {
                    if (a == null) {
                        return p;
                    }
                    if (p == null) {
                        return a;
                    }
                    a.addAll(p.getAccumulator());
                    return a;
                });


        JavaPairRDD<String, Result> pubs = publications.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> dss = datasets.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> sfw = software.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> orp = other.mapToPair(p -> new Tuple2<>(p.getId(),p));

        updateResult(pubs, toupdateresult, outputPath, "publication");
        updateResult(dss, toupdateresult, outputPath, "dataset");
        updateResult(sfw, toupdateresult, outputPath, "software");
        updateResult(orp, toupdateresult, outputPath, "otherresearchproduct");
        //we use leftOuterJoin because we want to rebuild the entire structure

*/

    }


    
    private static void createUpdateForResult(JavaPairRDD<String, TypedRow> toupdateresult, String outputPath, String type){
        toupdateresult.map(c -> {
            List<Country> countryList = new ArrayList<>();
            for (String country : c._2.getAccumulator()) {
                countryList.add(getCountry(country));
            }
            switch(type                ){
                case  "software":
                    Software s = new Software();
                    s.setId(c._1());
                    s.setCountry(countryList);
                    return s;
                case "publication":
                    break;
                case "dataset":
                    break;
                case "otherresearchproduct":
                    break;

            }
            return null;
        }).map(r ->new ObjectMapper().writeValueAsString(r))
                .saveAsTextFile(outputPath+"/"+type);
    }

    private static void updateResult(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult, String outputPath, String type) {
        results.leftOuterJoin(toupdateresult)
                .map(c -> {
                    Result oaf = c._2()._1();
                    List<Country> countryList = oaf.getCountry();
                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : countryList) {
                            countries.add(country.getClassid());
                        }
                        TypedRow t = c._2()._2().get();

                        for (String country : t.getAccumulator()) {
                            if (!countries.contains(country)) {
                                countryList.add(getCountry(country));
                            }

                        }
                        oaf.setCountry(countryList);
                    }

                    return oaf;
                })
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/"+type);
    }
    

}

