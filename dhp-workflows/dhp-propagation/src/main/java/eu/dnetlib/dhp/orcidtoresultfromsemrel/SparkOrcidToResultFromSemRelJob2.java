package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkOrcidToResultFromSemRelJob2 {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkOrcidToResultFromSemRelJob2.class.getResourceAsStream("/eu/dnetlib/dhp/orcidtoresultfromsemrel/input_orcidtoresult_parameters.json")));
        parser.parseArgument(args);
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkOrcidToResultFromSemRelJob2.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/orcidtoresult";

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
        boolean writeUpdate = TRUE.equals(parser.get("writeUpdate"));
        boolean saveGraph = TRUE.equals(parser.get("saveGraph"));

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));

        org.apache.spark.sql.Dataset<Dataset> dataset = spark.createDataset(sc.textFile(inputPath + "/dataset")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Dataset.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class));

        org.apache.spark.sql.Dataset<OtherResearchProduct> other = spark.createDataset(sc.textFile(inputPath + "/otherresearchproduct")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class));

        org.apache.spark.sql.Dataset<Software> software = spark.createDataset(sc.textFile(inputPath + "/software")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Software.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Software.class));

        org.apache.spark.sql.Dataset<Publication> publication = spark.createDataset(sc.textFile(inputPath + "/publication")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Publication.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Publication.class));


        relation.createOrReplaceTempView("relation");
        String query = "Select source, target " +
                "from relation " +
                "where datainfo.deletedbyinference = false " + getConstraintList(" relclass = '" , allowedsemrel);

        org.apache.spark.sql.Dataset<Row> result_result = spark.sql(query);

        publication.createOrReplaceTempView("publication");
        org.apache.spark.sql.Dataset<ResultOrcidList> pubs_with_orcid = getResultWithOrcid("publication", spark)
                .as(Encoders.bean(ResultOrcidList.class));

        dataset.createOrReplaceTempView("dataset");
        org.apache.spark.sql.Dataset<ResultOrcidList> dats_with_orcid = getResultWithOrcid("dataset", spark)
                .as(Encoders.bean(ResultOrcidList.class));

        other.createOrReplaceTempView("orp");
        org.apache.spark.sql.Dataset<ResultOrcidList> orp_with_orcid = getResultWithOrcid("orp", spark)
                .as(Encoders.bean(ResultOrcidList.class));

        dataset.createOrReplaceTempView("software");
        org.apache.spark.sql.Dataset<ResultOrcidList> software_with_orcid = getResultWithOrcid("software", spark)
                .as(Encoders.bean(ResultOrcidList.class));
        //get the results having at least one author pid we are interested in

        //target of the relation from at least one source with orcid.
        //the set of authors contains all those that have orcid and are related to target
        //from any source with allowed semantic relationship
        JavaPairRDD<String, List<AutoritativeAuthor>> target_authorlist_from_pubs = getTargetAutoritativeAuthorList(pubs_with_orcid);

        JavaPairRDD<String, List<AutoritativeAuthor>> target_authorlist_from_dats = getTargetAutoritativeAuthorList(dats_with_orcid);

        JavaPairRDD<String, List<AutoritativeAuthor>> target_authorlist_from_orp = getTargetAutoritativeAuthorList(orp_with_orcid);

        JavaPairRDD<String, List<AutoritativeAuthor>> target_authorlist_from_sw = getTargetAutoritativeAuthorList(software_with_orcid);

        if(writeUpdate){
            target_authorlist_from_dats.map(r -> new ObjectMapper().writeValueAsString(r))
                    .saveAsTextFile(outputPath + "/" + "update_dats");
            target_authorlist_from_pubs.map(r -> new ObjectMapper().writeValueAsString(r))
                    .saveAsTextFile(outputPath + "/" + "update_pubs");
            target_authorlist_from_orp.map(r -> new ObjectMapper().writeValueAsString(r))
                    .saveAsTextFile(outputPath + "/" + "update_orp");
            target_authorlist_from_sw.map(r -> new ObjectMapper().writeValueAsString(r))
                    .saveAsTextFile(outputPath + "/" + "update_sw");
        }
        
        if(saveGraph){
            sc.textFile(inputPath + "/publication")
                    .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Publication.class))
                    .mapToPair(p -> new Tuple2<>(p.getId(),p))
                    .leftOuterJoin(target_authorlist_from_pubs)
                    .map(c -> {
                        Result r = c._2()._1();
                        if(!c._2()._2().isPresent()){
                            return r;
                        }
                        List<eu.dnetlib.dhp.schema.oaf.Author> toenrich_author = r.getAuthor();
                        List<AutoritativeAuthor> autoritativeAuthors = c._2()._2().get();
                        for(eu.dnetlib.dhp.schema.oaf.Author author: toenrich_author){
                            if (!containsAllowedPid(author)){
                                enrichAuthor(author, autoritativeAuthors);
                            }
                        }
                        return r;
                    });
                            
        }

    }
    
    private static void enrichAuthor(eu.dnetlib.dhp.schema.oaf.Author a, List<AutoritativeAuthor> au){
        for (AutoritativeAuthor aa: au){
            if(enrichAuthor(aa, a)){
                return;
            }
        }

    }

//    private static JavaPairRDD<String, List<AutoritativeAuthor>> getTargetAutoritativeAuthorList(org.apache.spark.sql.Dataset<Row> result_result, org.apache.spark.sql.Dataset<ResultOrcidList> pubs_with_orcid) {
//        return pubs_with_orcid
//                .toJavaRDD()
//                .mapToPair(p -> new Tuple2<>(p.getId(), p.getAuthorList()))
//                .join(result_result.toJavaRDD().mapToPair(rel -> new Tuple2<>(rel.getString(0), rel.getString(1))))
//                .mapToPair(c -> new Tuple2<>(c._2._2(), c._2()._1()))
//                .reduceByKey((a, b) -> {
//                    if(a == null){
//                        return b;
//                    }
//                    if(b==null){
//                        return a;
//                    }
//
//                    Set<String> authSet = new HashSet<>();
//                    a.stream().forEach(au -> authSet.add(au.getOrcid()));
//
//                    b.stream().forEach(au -> {
//                                if (!authSet.contains(au.getOrcid())) {
//                                    a.add(au);
//                                }
//                            }
//                        );
//                    return a;
//                    });
//    }
private static JavaPairRDD<String, List<AutoritativeAuthor>> getTargetAutoritativeAuthorList( org.apache.spark.sql.Dataset<ResultOrcidList> pubs_with_orcid) {
    return pubs_with_orcid
            .toJavaRDD()
            .mapToPair(p -> new Tuple2<>(p.getResultId(), p.getAuthorList()))
            .reduceByKey((a, b) -> {
                if(a == null){
                    return b;
                }
                if(b==null){
                    return a;
                }
                Set<String> authSet = new HashSet<>();
                a.stream().forEach(au -> authSet.add(au.getOrcid()));

                b.stream().forEach(au -> {
                            if (!authSet.contains(au.getOrcid())) {
                                a.add(au);
                            }
                        }
                );
                return a;
            });
}

    private static org.apache.spark.sql.Dataset<Row> getResultWithOrcid(String table, SparkSession spark){
        String query = " select target, author " +
                " from (select id, collect_set(named_struct('name', name, 'surname', surname, 'fullname', fullname, 'orcid', orcid)) author " +
                " from ( " +
                " select id, MyT.fullname, MyT.name, MyT.surname, MyP.value orcid " +
                " from " + table +
                " lateral view explode (author) a as MyT " +
                " lateral view explode (MyT.pid) p as MyP " +
                " where MyP.qualifier.classid = 'ORCID') tmp " +
                " group by id) r_t " +
                " join (" +
                " select source, target " +
                " from relation " +
                " where datainfo.deletedbyinference = false  and (relclass = 'isSupplementedBy' or relclass = 'isSupplementTo') rel_rel " +
                " on source = id";

        return spark.sql(query);
    }


    private static boolean enrichAuthor(AutoritativeAuthor autoritative_author, eu.dnetlib.dhp.schema.oaf.Author author) {
        boolean toaddpid = false;

        if (StringUtils.isNoneEmpty(autoritative_author.getSurname())) {
            if (StringUtils.isNoneEmpty(author.getSurname())) {
                if (autoritative_author.getSurname().trim().equalsIgnoreCase(author.getSurname().trim())) {

                    //have the same surname. Check the name
                    if (StringUtils.isNoneEmpty(autoritative_author.getName())) {
                        if (StringUtils.isNoneEmpty(author.getName())) {
                            if (autoritative_author.getName().trim().equalsIgnoreCase(author.getName().trim())) {
                                toaddpid = true;
                            }
                            //they could be differently written (i.e. only the initials of the name in one of the two
                            if (autoritative_author.getName().trim().substring(0, 0).equalsIgnoreCase(author.getName().trim().substring(0, 0))) {
                                toaddpid = true;
                            }
                        }
                    }
                }
            }
        }
        if (toaddpid){
            StructuredProperty pid = new StructuredProperty();
            String aa_pid = autoritative_author.getOrcid();
            pid.setValue(aa_pid);
            pid.setQualifier(getQualifier(PROPAGATION_AUTHOR_PID, PROPAGATION_AUTHOR_PID ));
            pid.setDataInfo(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_ID, PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_NAME));
            if(author.getPid() == null){
                author.setPid(Arrays.asList(pid));
            }else{
                author.getPid().add(pid);
            }

        }
        return toaddpid;

    }


//    private static List<Author> enrichAuthors(List<Author> autoritative_authors, List<Author> to_enrich_authors, boolean filter){
////        List<Author> autoritative_authors = p._2()._2().get().getAuthors();
////        List<Author> to_enrich_authors = r.getAuthor();
//
//        return to_enrich_authors
//                .stream()
//                .map(a -> {
//                    if (filter) {
//                        if (containsAllowedPid(a)) {
//                            return a;
//                        }
//                    }
//
//                    List<Author> lst = autoritative_authors.stream()
//                            .map(aa -> enrichAuthor(aa, a)).filter(au -> !(au == null)).collect(Collectors.toList());
//                    if (lst.size() == 0) {
//                        return a;
//                    }
//                    return lst.get(0);//Each author can be enriched at most once. It cannot be the same as many different people
//
//                }).collect(Collectors.toList());
//    }
//
//    private static void writeResult(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult,
//                                    String outputPath, String type) {
//
//        results.join(toupdateresult)
//                .map(p -> {
//                    Result r = p._2()._1();
//
//                        List<Author> autoritative_authors = p._2()._2().getAuthors();
//                    List<eu.dnetlib.dhp.schema.oaf.Author> to_enrich_authors = r.getAuthor();
//
//                        r.setAuthor(enrichAutors(autoritative_authors, to_enrich_authors, false));
////                                .stream()
////                                .map(a -> {
////                                    if(filter) {
////                                        if (containsAllowedPid(a)) {
////                                            return a;
////                                        }
////                                    }
////
////                                    List<Author> lst = autoritative_authors.stream()
////                                            .map(aa -> enrichAuthor(aa, a)).filter(au -> !(au == null)).collect(Collectors.toList());
////                                    if(lst.size() == 0){
////                                        return a;
////                                    }
////                                    return lst.get(0);//Each author can be enriched at most once. It cannot be the same as many different people
////
////                                }).collect(Collectors.toList()));
//
//                    return r;
//                })
//                .map(p -> new ObjectMapper().writeValueAsString(p))
//                .saveAsTextFile(outputPath + "/" + type + "_update");
//    }


//    private static void updateResult(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult,
//                                     String outputPath, String type) {
//        results.leftOuterJoin(toupdateresult)
//                .map(p -> {
//                    Result r = p._2()._1();
//                    if (p._2()._2().isPresent()){
//                        List<AutoritativeAuthor> autoritative_authors = p._2()._2().get().getAuthors();
//                        List<eu.dnetlib.dhp.schema.oaf.Author> to_enrich_authors = r.getAuthor();
//
//                        r.setAuthor(enrichAutors(autoritative_authors, to_enrich_authors, true));
////                                .stream()
////                                .map(a -> {
////                                    if(filter) {
////                                        if (containsAllowedPid(a)) {
////                                            return a;
////                                        }
////                                    }
////
////                                    List<Author> lst = autoritative_authors.stream()
////                                            .map(aa -> enrichAuthor(aa, a)).filter(au -> !(au == null)).collect(Collectors.toList());
////                                    if(lst.size() == 0){
////                                        return a;
////                                    }
////                                    return lst.get(0);//Each author can be enriched at most once. It cannot be the same as many different people
////
////                                }).collect(Collectors.toList()));
//                    }
//                    return r;
//                })
//                .map(p -> new ObjectMapper().writeValueAsString(p))
//                .saveAsTextFile(outputPath+"/"+type);
//    }

    

    private static boolean containsAllowedPid(eu.dnetlib.dhp.schema.oaf.Author a) {
        for (StructuredProperty pid : a.getPid()) {
            if (PROPAGATION_AUTHOR_PID.equals(pid.getQualifier().getClassid())) {
                return true;
            }
        }
        return false;
    }

}


/*private ResultProtos.Result.Metadata.Builder searchMatch(List<FieldTypeProtos.Author> author_list){
        ResultProtos.Result.Metadata.Builder metadataBuilder = ResultProtos.Result.Metadata.newBuilder();
        boolean updated = false;

        for (FieldTypeProtos.Author a: author_list){
            FieldTypeProtos.Author.Builder author = searchAuthor(a, autoritative_authors);
            if(author != null){
                updated = true;
                metadataBuilder.addAuthor(author);
            }else{
                metadataBuilder.addAuthor(FieldTypeProtos.Author.newBuilder(a));
            }
        }
        if(updated)
            return metadataBuilder;
        return null;
    }
    private FieldTypeProtos.Author.Builder searchAuthor(FieldTypeProtos.Author a, List<FieldTypeProtos.Author> author_list){
        if(containsOrcid(a.getPidList()))
            return null;
        for(FieldTypeProtos.Author autoritative_author : author_list) {
                if (equals(autoritative_author, a)) {
                    if(!containsOrcid(a.getPidList()))
                        return update(a, autoritative_author);
                }
        }
        return  null;

    }

    private boolean containsOrcid(List<FieldTypeProtos.KeyValue> pidList){
        if(pidList == null)
            return false;
        return pidList
                .stream()
                .filter(kv -> kv.getKey().equals(PropagationConstants.AUTHOR_PID))
                .collect(Collectors.toList()).size() > 0;
    }
    */