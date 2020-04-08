package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkOrcidToResultFromSemRelJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkOrcidToResultFromSemRelJob.class.getResourceAsStream("/eu/dnetlib/dhp/orcidtoresultfromremrel/input_orcidtoresult_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkOrcidToResultFromSemRelJob.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/orcidtoresult";

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
        boolean writeUpdate = TRUE.equals(parser.get("writeUpdate"));
        boolean saveGraph = TRUE.equals(parser.get("saveGraph"));

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        JavaRDD<Relation> relations = sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).cache();

        JavaPairRDD<String, TypedRow> result_result = getResultResultSemRel(allowedsemrel, relations);

        JavaRDD<Publication> publications = sc.textFile(inputPath + "/publication")
                .map(item -> new ObjectMapper().readValue(item, Publication.class));
        JavaRDD<Dataset> datasets = sc.textFile(inputPath + "/dataset")
                .map(item -> new ObjectMapper().readValue(item, Dataset.class));
        JavaRDD<Software> software = sc.textFile(inputPath + "/software")
                .map(item -> new ObjectMapper().readValue(item, Software.class));
        JavaRDD<OtherResearchProduct> other = sc.textFile(inputPath + "/otherresearchproduct")
                .map(item -> new ObjectMapper().readValue(item, OtherResearchProduct.class));

        //get the results having at least one author pid we are interested in
        JavaPairRDD<String, TypedRow> resultswithorcid = publications.map(p -> getTypedRow(p))
                        .filter(p -> !(p == null))
                        .mapToPair(toPair())
                .union(datasets.map(p -> getTypedRow(p))
                        .filter(p -> !(p == null))
                        .mapToPair(toPair()))
                .union(software.map(p -> getTypedRow(p))
                        .filter(p -> !(p == null))
                        .mapToPair(toPair()))
                .union(other.map(p -> getTypedRow(p))
                        .filter(p -> !(p == null))
                        .mapToPair(toPair()));


        JavaPairRDD<String, TypedRow> to_add_orcid_to_result = resultswithorcid.join(result_result)
                .map(p -> p._2()._1().setSourceId(p._2()._2().getTargetId())) //associate the pid of the result (target) which should get the orcid to the typed row containing the authors with the orcid from the result(source)
                .mapToPair(toPair());

        JavaPairRDD<String, Result> pubs = publications.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> dss = datasets.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> sfw = software.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> orp = other.mapToPair(p -> new Tuple2<>(p.getId(),p));

        if(writeUpdate){
            writeResult(pubs, to_add_orcid_to_result, outputPath, "publication");
            writeResult(dss, to_add_orcid_to_result, outputPath, "dataset");
            writeResult(sfw, to_add_orcid_to_result, outputPath, "software");
            writeResult(orp, to_add_orcid_to_result, outputPath, "otherresearchproduct");
        }

        if (saveGraph){
            updateResult(pubs, to_add_orcid_to_result, outputPath, "publication");
            updateResult(dss, to_add_orcid_to_result, outputPath, "dataset");
            updateResult(sfw, to_add_orcid_to_result, outputPath, "software");
            updateResult(orp, to_add_orcid_to_result, outputPath, "otherresearchproduct");
        }


    }


    private static Author enrichAutor(Author autoritative_author, Author author) {
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
            for(StructuredProperty sp : autoritative_author.getPid()){
                if (PROPAGATION_AUTHOR_PID.equals(sp.getQualifier().getClassid())){
                    pid.setValue(sp.getValue());
                    pid.setQualifier(getQualifier(sp.getQualifier().getClassid(),sp.getQualifier().getClassname() ));
                    pid.setDataInfo(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_ID, PROPAGATION_ORCID_TO_RESULT_FROM_SEM_REL_CLASS_NAME));
                    if(author.getPid() == null){
                        author.setPid(Arrays.asList(pid));
                    }else{
                        author.getPid().add(pid);
                    }
                }
            }
            return author;
        }
        return null;
    }


    private static List<Author> enrichAutors(List<Author> autoritative_authors, List<Author> to_enrich_authors, boolean filter){
//        List<Author> autoritative_authors = p._2()._2().get().getAuthors();
//        List<Author> to_enrich_authors = r.getAuthor();

        return to_enrich_authors
                .stream()
                .map(a -> {
                    if (filter) {
                        if (containsAllowedPid(a)) {
                            return a;
                        }
                    }

                    List<Author> lst = autoritative_authors.stream()
                            .map(aa -> enrichAutor(aa, a)).filter(au -> !(au == null)).collect(Collectors.toList());
                    if (lst.size() == 0) {
                        return a;
                    }
                    return lst.get(0);//Each author can be enriched at most once. It cannot be the same as many different people

                }).collect(Collectors.toList());
    }

    private static void writeResult(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult,
                                    String outputPath, String type) {

        results.join(toupdateresult)
                .map(p -> {
                    Result r = p._2()._1();

                        List<Author> autoritative_authors = p._2()._2().getAuthors();
                        List<Author> to_enrich_authors = r.getAuthor();

                        r.setAuthor(enrichAutors(autoritative_authors, to_enrich_authors, false));
//                                .stream()
//                                .map(a -> {
//                                    if(filter) {
//                                        if (containsAllowedPid(a)) {
//                                            return a;
//                                        }
//                                    }
//
//                                    List<Author> lst = autoritative_authors.stream()
//                                            .map(aa -> enrichAutor(aa, a)).filter(au -> !(au == null)).collect(Collectors.toList());
//                                    if(lst.size() == 0){
//                                        return a;
//                                    }
//                                    return lst.get(0);//Each author can be enriched at most once. It cannot be the same as many different people
//
//                                }).collect(Collectors.toList()));

                    return r;
                })
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/" + type + "_update");
    }


    private static void updateResult(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult,
                                     String outputPath, String type) {
        results.leftOuterJoin(toupdateresult)
                .map(p -> {
                    Result r = p._2()._1();
                    if (p._2()._2().isPresent()){
                        List<Author> autoritative_authors = p._2()._2().get().getAuthors();
                        List<Author> to_enrich_authors = r.getAuthor();

                        r.setAuthor(enrichAutors(autoritative_authors, to_enrich_authors, true));
//                                .stream()
//                                .map(a -> {
//                                    if(filter) {
//                                        if (containsAllowedPid(a)) {
//                                            return a;
//                                        }
//                                    }
//
//                                    List<Author> lst = autoritative_authors.stream()
//                                            .map(aa -> enrichAutor(aa, a)).filter(au -> !(au == null)).collect(Collectors.toList());
//                                    if(lst.size() == 0){
//                                        return a;
//                                    }
//                                    return lst.get(0);//Each author can be enriched at most once. It cannot be the same as many different people
//
//                                }).collect(Collectors.toList()));
                    }
                    return r;
                })
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/"+type);
    }

    private static TypedRow getTypedRow(Result p) {
        TypedRow tp = new TypedRow();
        tp.setSourceId(p.getId());
        List<Author> authorList = p.getAuthor()
                .stream()
                .map(a -> {
                    if (a.getPid().stream().map(pid -> {
                        if (PROPAGATION_AUTHOR_PID.equals(pid.getQualifier().getClassid())) {
                            return a;
                        }
                        return null;
                    }).filter(aut -> !(aut == null)).collect(Collectors.toList()).size() > 0){
                        return a;
                    }
                    return null;
                }).filter(a -> !(a == null)).collect(Collectors.toList());
        tp.setAuthors(authorList);
        if(authorList.size() > 0){
            return tp;
        }
        return null;


    }

    private static boolean containsAllowedPid(Author a){


        return (a.getPid().stream().map(pid -> {
            if (PROPAGATION_AUTHOR_PID.equals(pid.getQualifier().getClassid())) {
                return true;
            }
            return false;
            }).filter(aut -> (aut == true)).collect(Collectors.toList()).size()) > 0;
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