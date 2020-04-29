package eu.dnetlib.dhp.oa.graph.raw.common;

import static eu.dnetlib.dhp.oa.graph.raw.common.OafMapperUtils.qualifier;

import eu.dnetlib.dhp.schema.oaf.Qualifier;

public class MigrationConstants {

  public static final Qualifier PUBLICATION_RESULTTYPE_QUALIFIER =
      qualifier("publication", "publication", "dnet:result_typologies", "dnet:result_typologies");
  public static final Qualifier DATASET_RESULTTYPE_QUALIFIER =
      qualifier(
          "dataset", "dataset",
          "dnet:result_typologies", "dnet:result_typologies");
  public static final Qualifier SOFTWARE_RESULTTYPE_QUALIFIER =
      qualifier(
          "software", "software",
          "dnet:result_typologies", "dnet:result_typologies");
  public static final Qualifier OTHER_RESULTTYPE_QUALIFIER =
      qualifier(
          "other", "other",
          "dnet:result_typologies", "dnet:result_typologies");
  public static final Qualifier REPOSITORY_PROVENANCE_ACTIONS =
      qualifier(
          "sysimport:crosswalk:repository", "sysimport:crosswalk:repository",
          "dnet:provenanceActions", "dnet:provenanceActions");
  public static final Qualifier ENTITYREGISTRY_PROVENANCE_ACTION =
      qualifier(
          "sysimport:crosswalk:entityregistry", "sysimport:crosswalk:entityregistry",
          "dnet:provenanceActions", "dnet:provenanceActions");
}
