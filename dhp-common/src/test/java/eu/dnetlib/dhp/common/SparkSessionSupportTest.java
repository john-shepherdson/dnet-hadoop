
package eu.dnetlib.dhp.common;

import static org.mockito.Mockito.*;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.ThrowingConsumer;
import java.util.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class SparkSessionSupportTest {

	@Nested
	class RunWithSparkSession {

		@Test
		public void shouldExecuteFunctionAndNotStopSparkSessionWhenSparkSessionIsNotManaged()
			throws Exception {
			// given
			SparkSession spark = mock(SparkSession.class);
			SparkConf conf = mock(SparkConf.class);
			Function<SparkConf, SparkSession> sparkSessionBuilder = mock(Function.class);
			when(sparkSessionBuilder.apply(conf)).thenReturn(spark);
			ThrowingConsumer<SparkSession, Exception> fn = mock(ThrowingConsumer.class);

			// when
			SparkSessionSupport.runWithSparkSession(sparkSessionBuilder, conf, false, fn);

			// then
			verify(sparkSessionBuilder).apply(conf);
			verify(fn).accept(spark);
			verify(spark, never()).stop();
		}

		@Test
		public void shouldExecuteFunctionAndStopSparkSessionWhenSparkSessionIsManaged()
			throws Exception {
			// given
			SparkSession spark = mock(SparkSession.class);
			SparkConf conf = mock(SparkConf.class);
			Function<SparkConf, SparkSession> sparkSessionBuilder = mock(Function.class);
			when(sparkSessionBuilder.apply(conf)).thenReturn(spark);
			ThrowingConsumer<SparkSession, Exception> fn = mock(ThrowingConsumer.class);

			// when
			SparkSessionSupport.runWithSparkSession(sparkSessionBuilder, conf, true, fn);

			// then
			verify(sparkSessionBuilder).apply(conf);
			verify(fn).accept(spark);
			verify(spark, times(1)).stop();
		}
	}
}
