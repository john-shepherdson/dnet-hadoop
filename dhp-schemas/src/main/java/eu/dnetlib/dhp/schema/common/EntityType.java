package eu.dnetlib.dhp.schema.common;

import eu.dnetlib.dhp.schema.oaf.OafEntity;

/** Actual entity types in the Graph */
public enum EntityType {
    publication,
    dataset,
    otherresearchproduct,
    software,
    datasource,
    organization,
    project;

    /**
     * Resolves the EntityType, given the relative class name
     *
     * @param clazz the given class name
     * @param <T> actual OafEntity subclass
     * @return the EntityType associated to the given class
     */
    public static <T extends OafEntity> EntityType fromClass(Class<T> clazz) {

        return EntityType.valueOf(clazz.getSimpleName().toLowerCase());
    }
}
