package eu.dnetlib.dhp.schema.common;

import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class ModelConstants {

    public static final String DNET_RESULT_TYPOLOGIES = "dnet:result_typologies";

    public static final String DATASET_RESULTTYPE_CLASSID = "dataset";
    public static final String PUBLICATION_RESULTTYPE_CLASSID = "publication";
    public static final String SOFTWARE_RESULTTYPE_CLASSID = "software";
    public static final String ORP_RESULTTYPE_CLASSID = "other";

    public static Qualifier PUBLICATION_DEFAULT_RESULTTYPE = new Qualifier();
    public static Qualifier DATASET_DEFAULT_RESULTTYPE = new Qualifier();
    public static Qualifier SOFTWARE_DEFAULT_RESULTTYPE = new Qualifier();
    public static Qualifier ORP_DEFAULT_RESULTTYPE = new Qualifier();

    static {
        PUBLICATION_DEFAULT_RESULTTYPE.setClassid(PUBLICATION_RESULTTYPE_CLASSID);
        PUBLICATION_DEFAULT_RESULTTYPE.setClassname(PUBLICATION_RESULTTYPE_CLASSID);
        PUBLICATION_DEFAULT_RESULTTYPE.setSchemeid(DNET_RESULT_TYPOLOGIES);
        PUBLICATION_DEFAULT_RESULTTYPE.setSchemename(DNET_RESULT_TYPOLOGIES);

        DATASET_DEFAULT_RESULTTYPE.setClassid(DATASET_RESULTTYPE_CLASSID);
        DATASET_DEFAULT_RESULTTYPE.setClassname(DATASET_RESULTTYPE_CLASSID);
        DATASET_DEFAULT_RESULTTYPE.setSchemeid(DNET_RESULT_TYPOLOGIES);
        DATASET_DEFAULT_RESULTTYPE.setSchemename(DNET_RESULT_TYPOLOGIES);

        SOFTWARE_DEFAULT_RESULTTYPE.setClassid(SOFTWARE_RESULTTYPE_CLASSID);
        SOFTWARE_DEFAULT_RESULTTYPE.setClassname(SOFTWARE_RESULTTYPE_CLASSID);
        SOFTWARE_DEFAULT_RESULTTYPE.setSchemeid(DNET_RESULT_TYPOLOGIES);
        SOFTWARE_DEFAULT_RESULTTYPE.setSchemename(DNET_RESULT_TYPOLOGIES);

        ORP_DEFAULT_RESULTTYPE.setClassid(ORP_RESULTTYPE_CLASSID);
        ORP_DEFAULT_RESULTTYPE.setClassname(ORP_RESULTTYPE_CLASSID);
        ORP_DEFAULT_RESULTTYPE.setSchemeid(DNET_RESULT_TYPOLOGIES);
        ORP_DEFAULT_RESULTTYPE.setSchemename(DNET_RESULT_TYPOLOGIES);
    }
}
