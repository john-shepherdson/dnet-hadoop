
package eu.dnetlib.dhp.common;

import java.io.Serializable;
import java.util.function.Supplier;

/** Provides serializable and throwing extensions to standard functional interfaces. */
public class FunctionalInterfaceSupport {

	private FunctionalInterfaceSupport() {
	}

	/**
	 * Serializable supplier of any kind of objects. To be used withing spark processing pipelines when supplying
	 * functions externally.
	 *
	 * @param <T>
	 */
	@FunctionalInterface
	public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
	}

	/**
	 * Extension of consumer accepting functions throwing an exception.
	 *
	 * @param <T>
	 * @param <E>
	 */
	@FunctionalInterface
	public interface ThrowingConsumer<T, E extends Exception> {
		void accept(T t) throws E;
	}

	/**
	 * Extension of supplier accepting functions throwing an exception.
	 *
	 * @param <T>
	 * @param <E>
	 */
	@FunctionalInterface
	public interface ThrowingSupplier<T, E extends Exception> {
		T get() throws E;
	}

	/**
	 * Extension of runnable accepting functions throwing an exception.
	 *
	 * @param <E>
	 */
	@FunctionalInterface
	public interface ThrowingRunnable<E extends Exception> {
		void run() throws E;
	}
}
