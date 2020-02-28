package eu.dnetlib.dhp.orcidtoresultfromsemrel;

public class SparkOrcidToReseltFromSemRelJob {
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