package eu.dnetlib.dhp.actionmanager;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;

import java.util.function.BiFunction;

public class OafMergeAndGet {

    public enum Strategy {
        MERGE_FROM_AND_GET, SELECT_NEWER_AND_GET
    }

    public static <T extends Oaf> SerializableSupplier<BiFunction<T, T, T>> functionFor(Strategy strategy) {
        switch (strategy) {
            case MERGE_FROM_AND_GET:
                return () -> (x, y) -> {
                    if (x instanceof Relation) {
                        ((Relation) x).mergeFrom((Relation) y);
                        return x;
                    }
                    ((OafEntity) x).mergeFrom((OafEntity) y);
                    return x;
                };
            case SELECT_NEWER_AND_GET:
                return () -> (x, y) -> {
                    if (x.getLastupdatetimestamp() > y.getLastupdatetimestamp()) {
                        return x;
                    }
                    return y;
                };
        }
        throw new RuntimeException();
    }

}
