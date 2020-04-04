package eu.dnetlib.dhp.schema.common;

import eu.dnetlib.dhp.schema.oaf.Oaf;

/**
 * Inheritance utility methods.
 */
public class ModelSupport {

    private ModelSupport() {
    }

    /**
     * Checks subclass-superclass relationship.
     *
     * @param subClazzObject   Subclass object instance
     * @param superClazzObject Superclass object instance
     * @param <X>              Subclass type
     * @param <Y>              Superclass type
     * @return True if X is a subclass of Y
     */
    public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(X subClazzObject, Y superClazzObject) {
        return isSubClass(subClazzObject.getClass(), superClazzObject.getClass());
    }

    /**
     * Checks subclass-superclass relationship.
     *
     * @param subClazzObject Subclass object instance
     * @param superClazz     Superclass class
     * @param <X>            Subclass type
     * @param <Y>            Superclass type
     * @return True if X is a subclass of Y
     */
    public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(X subClazzObject, Class<Y> superClazz) {
        return isSubClass(subClazzObject.getClass(), superClazz);
    }

    /**
     * Checks subclass-superclass relationship.
     *
     * @param subClazz   Subclass class
     * @param superClazz Superclass class
     * @param <X>        Subclass type
     * @param <Y>        Superclass type
     * @return True if X is a subclass of Y
     */
    public static <X extends Oaf, Y extends Oaf> Boolean isSubClass(Class<X> subClazz, Class<Y> superClazz) {
        return superClazz.isAssignableFrom(subClazz);
    }
}