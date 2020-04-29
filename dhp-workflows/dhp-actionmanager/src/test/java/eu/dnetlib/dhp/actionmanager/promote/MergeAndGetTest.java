package eu.dnetlib.dhp.actionmanager.promote;

import static eu.dnetlib.dhp.actionmanager.promote.MergeAndGet.Strategy;
import static eu.dnetlib.dhp.actionmanager.promote.MergeAndGet.functionFor;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import eu.dnetlib.dhp.common.FunctionalInterfaceSupport.SerializableSupplier;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class MergeAndGetTest {

  @Nested
  class MergeFromAndGetStrategy {

    @Test
    public void shouldThrowForOafAndOaf() {
      // given
      Oaf a = mock(Oaf.class);
      Oaf b = mock(Oaf.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForOafAndRelation() {
      // given
      Oaf a = mock(Oaf.class);
      Relation b = mock(Relation.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForOafAndOafEntity() {
      // given
      Oaf a = mock(Oaf.class);
      OafEntity b = mock(OafEntity.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForRelationAndOaf() {
      // given
      Relation a = mock(Relation.class);
      Oaf b = mock(Oaf.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForRelationAndOafEntity() {
      // given
      Relation a = mock(Relation.class);
      OafEntity b = mock(OafEntity.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldBehaveProperlyForRelationAndRelation() {
      // given
      Relation a = mock(Relation.class);
      Relation b = mock(Relation.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      Oaf x = fn.get().apply(a, b);
      assertTrue(Relation.class.isAssignableFrom(x.getClass()));
      verify(a).mergeFrom(b);
      assertEquals(a, x);
    }

    @Test
    public void shouldThrowForOafEntityAndOaf() {
      // given
      OafEntity a = mock(OafEntity.class);
      Oaf b = mock(Oaf.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForOafEntityAndRelation() {
      // given
      OafEntity a = mock(OafEntity.class);
      Relation b = mock(Relation.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForOafEntityAndOafEntityButNotSubclasses() {
      // given
      class OafEntitySub1 extends OafEntity {}
      class OafEntitySub2 extends OafEntity {}

      OafEntitySub1 a = mock(OafEntitySub1.class);
      OafEntitySub2 b = mock(OafEntitySub2.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldBehaveProperlyForOafEntityAndOafEntity() {
      // given
      OafEntity a = mock(OafEntity.class);
      OafEntity b = mock(OafEntity.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn = functionFor(Strategy.MERGE_FROM_AND_GET);

      // then
      Oaf x = fn.get().apply(a, b);
      assertTrue(OafEntity.class.isAssignableFrom(x.getClass()));
      verify(a).mergeFrom(b);
      assertEquals(a, x);
    }
  }

  @Nested
  class SelectNewerAndGetStrategy {

    @Test
    public void shouldThrowForOafEntityAndRelation() {
      // given
      OafEntity a = mock(OafEntity.class);
      Relation b = mock(Relation.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn =
          functionFor(Strategy.SELECT_NEWER_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForRelationAndOafEntity() {
      // given
      Relation a = mock(Relation.class);
      OafEntity b = mock(OafEntity.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn =
          functionFor(Strategy.SELECT_NEWER_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowForOafEntityAndResult() {
      // given
      OafEntity a = mock(OafEntity.class);
      Result b = mock(Result.class);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn =
          functionFor(Strategy.SELECT_NEWER_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldThrowWhenSuperTypeIsNewerForResultAndOafEntity() {
      // given
      // real types must be used because subclass-superclass resolution does not work for
      // mocks
      Dataset a = new Dataset();
      a.setLastupdatetimestamp(1L);
      Result b = new Result();
      b.setLastupdatetimestamp(2L);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn =
          functionFor(Strategy.SELECT_NEWER_AND_GET);

      // then
      assertThrows(RuntimeException.class, () -> fn.get().apply(a, b));
    }

    @Test
    public void shouldShouldReturnLeftForOafEntityAndOafEntity() {
      // given
      OafEntity a = mock(OafEntity.class);
      when(a.getLastupdatetimestamp()).thenReturn(1L);
      OafEntity b = mock(OafEntity.class);
      when(b.getLastupdatetimestamp()).thenReturn(2L);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn =
          functionFor(Strategy.SELECT_NEWER_AND_GET);

      // then
      Oaf x = fn.get().apply(a, b);
      assertTrue(OafEntity.class.isAssignableFrom(x.getClass()));
      assertEquals(b, x);
    }

    @Test
    public void shouldShouldReturnRightForOafEntityAndOafEntity() {
      // given
      OafEntity a = mock(OafEntity.class);
      when(a.getLastupdatetimestamp()).thenReturn(2L);
      OafEntity b = mock(OafEntity.class);
      when(b.getLastupdatetimestamp()).thenReturn(1L);

      // when
      SerializableSupplier<BiFunction<Oaf, Oaf, Oaf>> fn =
          functionFor(Strategy.SELECT_NEWER_AND_GET);

      // then
      Oaf x = fn.get().apply(a, b);
      assertTrue(OafEntity.class.isAssignableFrom(x.getClass()));
      assertEquals(a, x);
    }
  }
}
