package eu.dnetlib.dhp.actionmanager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class PromoteActionSetFromHDFSFunctionsTest {

    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(PromoteActionSetFromHDFSFunctionsTest.class.getSimpleName());
        conf.set("spark.driver.host", "localhost");

        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterClass
    public static void afterClass() {
        spark.stop();
    }

    @Test
    public void shouldJoinWithActionPayloadUsingIdAndMerge() {
        // given
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";
        String id4 = "id4";
        List<OafImpl> oafData = Arrays.asList(
                createOafImpl(id1), createOafImpl(id2), createOafImpl(id3), createOafImpl(id4)
        );
        Dataset<OafImpl> oafDS = spark.createDataset(oafData, Encoders.bean(OafImpl.class));

        List<String> actionPayloadData = Arrays.asList(
                createActionPayload(id1),
                createActionPayload(id2), createActionPayload(id2),
                createActionPayload(id3), createActionPayload(id3), createActionPayload(id3)
        );
        Dataset<String> actionPayloadDS = spark.createDataset(actionPayloadData, Encoders.STRING());

        SerializableSupplier<Function<OafImpl, String>> oafIdFn = () -> OafImpl::getId;
        SerializableSupplier<BiFunction<String, Class<OafImpl>, OafImpl>> actionPayloadToOafFn = () -> (s, clazz) -> {
            try {
                JsonNode jsonNode = new ObjectMapper().readTree(s);
                String id = jsonNode.at("/id").asText();
                return createOafImpl(id);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        SerializableSupplier<BiFunction<OafImpl, OafImpl, OafImpl>> mergeAndGetFn = () -> (x, y) -> {
            x.mergeFrom(y);
            return x;
        };

        // when
        List<OafImpl> results = PromoteActionSetFromHDFSFunctions
                .joinOafEntityWithActionPayloadAndMerge(oafDS,
                        actionPayloadDS,
                        oafIdFn,
                        actionPayloadToOafFn,
                        mergeAndGetFn,
                        OafImpl.class)
                .collectAsList();
//        System.out.println(results.stream().map(x -> String.format("%s:%d", x.getId(), x.merged)).collect(Collectors.joining(",")));

        // then
        assertEquals(7, results.size());
        results.forEach(result -> {
            switch (result.getId()) {
                case "id1":
                case "id2":
                case "id3":
                    assertEquals(2, result.merged);
                    break;
                case "id4":
                    assertEquals(1, result.merged);
                    break;
            }
        });
    }

    @Test
    public void shouldGroupOafEntitiesByIdAndMergeWithinGroup() {
        // given
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";
        List<OafImpl> oafData = Arrays.asList(
                createOafImpl(id1),
                createOafImpl(id2), createOafImpl(id2),
                createOafImpl(id3), createOafImpl(id3), createOafImpl(id3)
        );
        Dataset<OafImpl> oafDS = spark.createDataset(oafData, Encoders.bean(OafImpl.class));
        SerializableSupplier<Function<OafImpl, String>> idFn = () -> OafImpl::getId;
        SerializableSupplier<BiFunction<OafImpl, OafImpl, OafImpl>> mergeAndGetFn = () -> (x, y) -> {
            x.mergeFrom(y);
            return x;
        };

        // when
        List<OafImpl> results = PromoteActionSetFromHDFSFunctions
                .groupOafByIdAndMerge(oafDS,
                        idFn,
                        mergeAndGetFn,
                        OafImpl.class)
                .collectAsList();
//        System.out.println(results.stream().map(x -> String.format("%s:%d", x.getId(), x.merged)).collect(Collectors.joining(",")));

        // then
        assertEquals(3, results.size());
        results.forEach(result -> {
            switch (result.getId()) {
                case "id1":
                    assertEquals(1, result.merged);
                    break;
                case "id2":
                    assertEquals(2, result.merged);
                    break;
                case "id3":
                    assertEquals(3, result.merged);
                    break;
            }
        });
    }

    public static class OafImpl extends Oaf {
        private String id;
        private int merged = 1;

        public void mergeFrom(Oaf e) {
            merged += ((OafImpl) e).merged;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getMerged() {
            return merged;
        }

        public void setMerged(int merged) {
            this.merged = merged;
        }
    }

    private static OafImpl createOafImpl(String id) {
        OafImpl x = new OafImpl();
        x.setId(id);
        return x;
    }

    private static String createActionPayload(String id) {
        return String.format("{\"id\":\"%s\"}", id);
    }
}
