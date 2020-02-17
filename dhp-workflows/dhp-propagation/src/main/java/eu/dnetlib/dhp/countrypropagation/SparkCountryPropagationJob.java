package eu.dnetlib.dhp.countrypropagation;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.File;
import java.util.*;

public class SparkCountryPropagationJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkCountryPropagationJob.class.getResourceAsStream("/eu/dnetlib/dhp/countrypropagation/input_countrypropagation_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkCountryPropagationJob.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/countrytoresultfrominstitutionalrepositories";

        File directory = new File(outputPath);

        if(!directory.exists()){
            directory.mkdirs();
        }

        List<String> whitelist = Arrays.asList(parser.get("whitelist").split(";"));
        List<String> allowedtypes = Arrays.asList(parser.get("allowedtypes").split(";"));


        JavaPairRDD<String, TypedRow> organizations = sc.sequenceFile(inputPath + "/organization", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Organization.class))
                .filter(org -> !org.getDataInfo().getDeletedbyinference())
                .map(org -> new TypedRow().setSourceId(org.getId()).setCountry(org.getCountry().getClassid()))
                .mapToPair(toPair());

        JavaPairRDD<String, TypedRow> organization_datasource = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class))
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter(r -> "datasourceOrganization".equals(r.getRelClass()) && "isProvidedBy".equals(r.getRelType()))
                .map(r -> new TypedRow().setSourceId(r.getSource()).setTargetId(r.getTarget()))
                .mapToPair(toPair());

        JavaPairRDD<String, TypedRow> datasources = sc.sequenceFile(inputPath + "/datasource", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Datasource.class))
                .filter(ds -> whitelist.contains(ds.getId()) || allowedtypes.contains(ds.getDatasourcetype().getClassid()))
                .map(ds -> new TypedRow().setSourceId(ds.getId()))
                .mapToPair(toPair());


        JavaRDD<Publication> publications = sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class));
        JavaRDD<Dataset> datasets = sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class));
        JavaRDD<Software> software = sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class));
        JavaRDD<OtherResearchProduct> other = sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class));

        JavaPairRDD<String, TypedRow> datasource_results = publications
                .map(oaf -> getTypedRows(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                })
                .union(datasets
                        .map(oaf -> getTypedRows(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }))
                .union(software
                .map(oaf -> getTypedRows(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                }))
                .union(other
                        .map(oaf -> getTypedRows(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }));

        JavaPairRDD<String, OafEntity> pubs = publications.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, OafEntity> dss = datasets.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, OafEntity> sfw = software.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, OafEntity> orp = other.mapToPair(p -> new Tuple2<>(p.getId(),p));

        JavaPairRDD<String, TypedRow> datasource_country = organizations.join(organization_datasource)
                .map(x -> x._2()._1().setSourceId(x._2()._2().getTargetId())) // (OrganizationId,(TypedRow for Organization, TypedRow for Relation)
                .mapToPair(toPair()); //(DatasourceId, TypedRowforOrganziation)


        JavaPairRDD<String, TypedRow> alloweddatasources_country = datasources.join(datasource_country)
                .mapToPair(ds -> new Tuple2<>(ds._1(), ds._2()._2()));


        JavaPairRDD<String,TypedRow> toupdateresult = alloweddatasources_country.join(datasource_results)
                .map(u -> u._2()._2().setCountry(u._2()._1().getCountry()))
                .mapToPair(toPair())
                .reduceByKey((a, p) -> {
                    if (a == null) {
                        return p;
                    }
                    if (p == null) {
                        return a;
                    }
                    HashSet<String> countries = new HashSet();
                    countries.addAll(Arrays.asList(a.getCountry().split(";")));
                    countries.addAll(Arrays.asList(p.getCountry().split(";")));
                    String country = new String();
                    for (String c : countries) {
                        country += c + ";";
                    }

                    return a.setCountry(country);
                });

        updateResult(pubs, toupdateresult, outputPath, "publication");
        updateResult(dss, toupdateresult, outputPath, "dataset");
        updateResult(sfw, toupdateresult, outputPath, "software");
        updateResult(orp, toupdateresult, outputPath, "otherresearchproduct");
        //we use leftOuterJoin because we want to rebuild the entire structure

    }

    private static void updateResult(JavaPairRDD<String, OafEntity> results, JavaPairRDD<String, TypedRow> toupdateresult, String outputPath, String type) {
        results.leftOuterJoin(toupdateresult)
                .map(c -> {
                    OafEntity oaf = c._2()._1();
                    List<Qualifier> qualifierList = null;
                    if (oaf.getClass() == Publication.class) {
                        qualifierList = ((Publication) oaf).getCountry();

                    }
                    if (oaf.getClass() == Dataset.class){
                        qualifierList = ((Dataset) oaf).getCountry();
                    }

                    if (oaf.getClass() == Software.class){
                        qualifierList = ((Software) oaf).getCountry();
                    }

                    if (oaf.getClass() == OtherResearchProduct.class){
                        qualifierList = ((OtherResearchProduct) oaf).getCountry();
                    }

                    if (c._2()._2().isPresent()) {
                        HashSet<String> countries = new HashSet<>();
                        for (Qualifier country : qualifierList) {
                            countries.add(country.getClassid());
                        }
                        TypedRow t = c._2()._2().get();

                        for (String country : t.getCountry().split(";")) {
                            if (!countries.contains(country)) {
                                Qualifier q = new Qualifier();
                                q.setClassid(country);
                                qualifierList.add(q);
                            }

                        }
                        if (oaf.getClass() == Publication.class) {
                            ((Publication) oaf).setCountry(qualifierList);
                            return (Publication) oaf;

                        }
                        if (oaf.getClass() == Dataset.class){
                            ((Dataset) oaf).setCountry(qualifierList);
                            return (Dataset) oaf;
                        }

                        if (oaf.getClass() == Software.class){
                          ((Software) oaf).setCountry(qualifierList);
                            return (Software) oaf;
                        }

                        if (oaf.getClass() == OtherResearchProduct.class){
                           ((OtherResearchProduct) oaf).setCountry(qualifierList);
                            return (OtherResearchProduct) oaf;
                        }
                    }


                    return null;
                })
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/"+type);
    }

    private static List<TypedRow> getTypedRows(OafEntity oaf) {
        List<TypedRow> lst = new ArrayList<>();
        Set<String> datasources_provenance = new HashSet<>();
        List<Instance> instanceList = null;
        String type = "";
        if (oaf.getClass() == Publication.class) {
            instanceList = ((Publication) oaf).getInstance();
            type = "publication";
        }
        if (oaf.getClass() == Dataset.class){
            instanceList = ((Dataset)oaf).getInstance();
            type = "dataset";
        }

        if (oaf.getClass() == Software.class){
            instanceList = ((Software)oaf).getInstance();
            type = "software";
        }

        if (oaf.getClass() == OtherResearchProduct.class){
            instanceList = ((OtherResearchProduct)oaf).getInstance();
            type = "otherresearchproduct";
        }


        for (Instance i : instanceList) {
            datasources_provenance.add(i.getCollectedfrom().getKey());
            datasources_provenance.add(i.getHostedby().getKey());
        }
        for (String dsId : datasources_provenance) {
            lst.add(new TypedRow().setSourceId(dsId).setTargetId(oaf.getId()).setType(type));
        }
        return lst;
    }


    private static JavaPairRDD<String, TypedRow> getResults(JavaSparkContext sc , String inputPath){

        return
                sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                    .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class))
                        .filter(ds -> !ds.getDataInfo().getDeletedbyinference())
                    .map(oaf -> new TypedRow().setType("dataset").setSourceId(oaf.getId()))
                    .mapToPair(toPair())
                .union(sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                        .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class))
                        .filter(o -> !o.getDataInfo().getDeletedbyinference())
                        .map(oaf -> new TypedRow().setType("otherresearchproduct").setSourceId(oaf.getId()))
                        .mapToPair(toPair()))
                .union(sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                        .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class))
                        .filter(s -> !s.getDataInfo().getDeletedbyinference())
                        .map(oaf -> new TypedRow().setType("software").setSourceId(oaf.getId()))
                        .mapToPair(toPair()))
                .union(sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class))
                        .filter(p -> !p.getDataInfo().getDeletedbyinference())
                                .map(oaf -> new TypedRow().setType("publication").setSourceId(oaf.getId()))
                                .mapToPair(toPair()));


    }



    private static PairFunction<TypedRow, String, TypedRow> toPair() {
        return e -> new Tuple2<>( e.getSourceId(), e);

        };


}

