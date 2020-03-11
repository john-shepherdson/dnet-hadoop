package eu.dnetlib.dhp.actionmanager;

import java.io.Serializable;
import java.util.function.Supplier;

@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
