package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.getResultResultSemRel;

public class SparkOrcidToResultFromSemRelJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkOrcidToResultFromSemRelJob.class.getResourceAsStream("/eu/dnetlib/dhp/orcidtoresultfromsemrel/input_orcid" +
                "toresult_parameters.json")));
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

        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        JavaRDD<Relation> relations = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class)).cache();

        JavaPairRDD<String, TypedRow> result_result = getResultResultSemRel(allowedsemrel, relations);
    }
}
/*
public class PropagationOrcidToResultMapper extends TableMapper<ImmutableBytesWritable, Text> {
    private static final Log log = LogFactory.getLog(PropagationOrcidToResultMapper.class); // NOPMD by marko on 11/24/08 5:02 PM
    private Text valueOut;
    private ImmutableBytesWritable keyOut;
    private String[] sem_rels;
    private String trust;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        valueOut = new Text();
        keyOut = new ImmutableBytesWritable();

        sem_rels = context.getConfiguration().getStrings("propagatetoorcid.semanticrelations", DEFAULT_RESULT_RELATION_SET);
        trust = context.getConfiguration().get("propagatetoorcid.trust","0.85");

    }

    @Override
    protected void map(final ImmutableBytesWritable keyIn, final Result value, final Context context) throws IOException, InterruptedException {
        final TypeProtos.Type type = OafRowKeyDecoder.decode(keyIn.copyBytes()).getType();
        final OafProtos.OafEntity entity = getEntity(value, type);//getEntity already verified that it is not delByInference


        if (entity != null) {

            if (type == TypeProtos.Type.result){
                Set<String> result_result = new HashSet<>();
                //verifico se il risultato ha una relazione semantica verso uno o piu' risultati.
                //per ogni risultato linkato con issupplementto o issupplementedby emetto:
                // id risultato linkato come chiave,
                // id risultato oggetto del mapping e lista degli autori del risultato oggetto del mapper come value
                for(String sem : sem_rels){
                     result_result.addAll(getRelationTarget(value, sem, context, COUNTER_PROPAGATION));
                }
                if(!result_result.isEmpty()){
                    List<String> authorlist = getAuthorList(entity.getResult().getMetadata().getAuthorList());
                    Emit e = new Emit();
                    e.setId(Bytes.toString(keyIn.get()));
                    e.setAuthor_list(authorlist);
                    valueOut.set(Value.newInstance(new Gson().toJson(e, Emit.class),
                            trust,
                            Type.fromsemrel).toJson());
                    for (String result: result_result){
                        keyOut.set(Bytes.toBytes(result));
                        context.write(keyOut,valueOut);
                        context.getCounter(COUNTER_PROPAGATION,"emit for sem_rel").increment(1);
                    }

                    //emetto anche id dell'oggetto del mapper come chiave e lista degli autori come valore
                        e.setId(keyIn.toString());
                        e.setAuthor_list(authorlist);
                        valueOut.set(Value.newInstance(new Gson().toJson(e, Emit.class), trust, Type.fromresult).toJson());
                        context.write(keyIn, valueOut);
                        context.getCounter(COUNTER_PROPAGATION,"emit for result with orcid").increment(1);

                }
            }

        }
    }

    private List<String> getAuthorList(List<FieldTypeProtos.Author> author_list){

        return author_list.stream().map(a -> new JsonFormat().printToString(a)).collect(Collectors.toList());

    }



}
 */