package eu.dnetlib.dhp.common;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.ThrowingRunnable;
import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.ThrowingSupplier;

/** Exception handling utility methods. */
public class ThrowingSupport {

  private ThrowingSupport() {}

  /**
   * Executes given runnable and rethrows any exceptions as RuntimeException.
   *
   * @param fn Runnable to be executed
   * @param <E> Type of exception thrown
   */
  public static <E extends Exception> void rethrowAsRuntimeException(ThrowingRunnable<E> fn) {
    try {
      fn.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes given runnable and rethrows any exceptions as RuntimeException with custom message.
   *
   * @param fn Runnable to be executed
   * @param msg Message to be set for rethrown exception
   * @param <E> Type of exception thrown
   */
  public static <E extends Exception> void rethrowAsRuntimeException(
      ThrowingRunnable<E> fn, String msg) {
    try {
      fn.run();
    } catch (Exception e) {
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * Executes given supplier and rethrows any exceptions as RuntimeException.
   *
   * @param fn Supplier to be executed
   * @param <T> Type of returned value
   * @param <E> Type of exception thrown
   * @return Result of supplier execution
   */
  public static <T, E extends Exception> T rethrowAsRuntimeException(ThrowingSupplier<T, E> fn) {
    try {
      return fn.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes given supplier and rethrows any exceptions as RuntimeException with custom message.
   *
   * @param fn Supplier to be executed
   * @param msg Message to be set for rethrown exception
   * @param <T> Type of returned value
   * @param <E> Type of exception thrown
   * @return Result of supplier execution
   */
  public static <T, E extends Exception> T rethrowAsRuntimeException(
      ThrowingSupplier<T, E> fn, String msg) {
    try {
      return fn.get();
    } catch (Exception e) {
      throw new RuntimeException(msg, e);
    }
  }
}
