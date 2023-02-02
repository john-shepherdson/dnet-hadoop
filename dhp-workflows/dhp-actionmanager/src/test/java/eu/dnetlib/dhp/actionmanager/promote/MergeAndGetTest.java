
package eu.dnetlib.dhp.actionmanager.promote;

import static eu.dnetlib.dhp.actionmanager.promote.MergeAndGet.Strategy;
import static eu.dnetlib.dhp.actionmanager.promote.MergeAndGet.functionFor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.function.BiFunction;

import eu.dnetlib.dhp.schema.oaf.utils.MergeUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableSupplier;
import eu.dnetlib.dhp.schema.oaf.*;

public class MergeAndGetTest {

	@Nested
	class MergeFromAndGetStrategy {

		@Test
		void shouldThrowForOafAndOaf() {
			// given
			Oaf a = mock(Oaf.class);
			Oaf b = mock(Oaf.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForOafAndRelation() {
			// given
			Oaf a = mock(Oaf.class);
			Relation b = mock(Relation.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForOafAndOafEntity() {
			// given
			Oaf a = mock(Oaf.class);
			Entity b = mock(Entity.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForRelationAndOaf() {
			// given
			Relation a = mock(Relation.class);
			Oaf b = mock(Oaf.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForRelationAndOafEntity() {
			// given
			Relation a = mock(Relation.class);
			Entity b = mock(Entity.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldBehaveProperlyForRelationAndRelation() {
			// given
			Relation a = mock(Relation.class);
			Relation b = mock(Relation.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			Oaf x = fn.get().apply(a, b);
			assertTrue(Relation.class.isAssignableFrom(x.getClass()));
			//verify(a).mergeFrom(b);
			a = MergeUtils.mergeRelation(verify(a), b);
			assertEquals(a, x);
		}

		@Test
		void shouldThrowForOafEntityAndOaf() {
			// given
			Entity a = mock(Entity.class);
			Oaf b = mock(Oaf.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForOafEntityAndRelation() {
			// given
			Entity a = mock(Entity.class);
			Relation b = mock(Relation.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForOafEntityAndOafEntityButNotSubclasses() {
			// given
			class OafEntitySub1 extends Entity {
			}
			class OafEntitySub2 extends Entity {
			}

			OafEntitySub1 a = mock(OafEntitySub1.class);
			OafEntitySub2 b = mock(OafEntitySub2.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldBehaveProperlyForOafEntityAndOafEntity() {
			// given
			Entity a = mock(Entity.class);
			Entity b = mock(Entity.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

			// then
			Oaf x = fn.get().apply(a, b);
			assertTrue(Entity.class.isAssignableFrom(x.getClass()));
			a = MergeUtils.mergeEntity(verify(a), b);
			assertEquals(a, x);
		}
	}

	@Nested
	class SelectNewerAndGetStrategy {

		@Test
		void shouldThrowForOafEntityAndRelation() {
			// given
			Entity a = mock(Entity.class);
			Relation b = mock(Relation.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.SELECT_NEWER_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForRelationAndOafEntity() {
			// given
			Relation a = mock(Relation.class);
			Entity b = mock(Entity.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.SELECT_NEWER_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowForOafEntityAndResult() {
			// given
			Entity a = mock(Entity.class);
			Result b = mock(Result.class);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.SELECT_NEWER_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldThrowWhenSuperTypeIsNewerForResultAndOafEntity() {
			// given
			// real types must be used because subclass-superclass resolution does not work for
			// mocks
			Dataset a = new Dataset();
			a.setLastupdatetimestamp(1L);
			Result b = new Result();
			b.setLastupdatetimestamp(2L);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.SELECT_NEWER_AND_GET);

			// then
			assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
		}

		@Test
		void shouldShouldReturnLeftForOafEntityAndOafEntity() {
			// given
			Entity a = mock(Entity.class);
			when(a.getLastupdatetimestamp()).thenReturn(1L);
			Entity b = mock(Entity.class);
			when(b.getLastupdatetimestamp()).thenReturn(2L);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.SELECT_NEWER_AND_GET);

			// then
			Oaf x = fn.get().apply(a, b);
			assertTrue(Entity.class.isAssignableFrom(x.getClass()));
			assertEquals(b, x);
		}

		@Test
		void shouldShouldReturnRightForOafEntityAndOafEntity() {
			// given
			Entity a = mock(Entity.class);
			when(a.getLastupdatetimestamp()).thenReturn(2L);
			Entity b = mock(Entity.class);
			when(b.getLastupdatetimestamp()).thenReturn(1L);

			// when
			SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.SELECT_NEWER_AND_GET);

			// then
			Oaf x = fn.get().apply(a, b);
			assertTrue(Entity.class.isAssignableFrom(x.getClass()));
			assertEquals(a, x);
		}
	}
}
