package eu.dnetlib.dhp.actionmanager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.get_json_object;
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
    public void shouldGroupOafEntitiesByIdAndMergeWithinGroup() {
        // given
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";
        List<OafEntityImpl> entityData = Arrays.asList(
                createOafEntityImpl(id1),
                createOafEntityImpl(id2), createOafEntityImpl(id2),
                createOafEntityImpl(id3), createOafEntityImpl(id3), createOafEntityImpl(id3)
        );
        Dataset<OafEntityImpl> entityDS = spark.createDataset(entityData, Encoders.bean(OafEntityImpl.class));

        // when
        List<OafEntityImpl> results = PromoteActionSetFromHDFSFunctions
                .groupEntitiesByIdAndMerge(entityDS, OafEntityImpl.class)
                .collectAsList();
        System.out.println(results.stream().map(x -> String.format("%s:%d", x.getId(), x.merged)).collect(Collectors.joining(",")));

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

    @Test
    public void shouldJoinWithActionPayloadUsingIdAndMerge() {
        // given
        String id1 = "id1";
        String id2 = "id2";
        String id3 = "id3";
        String id4 = "id4";
        List<OafEntityImpl> entityData = Arrays.asList(
                createOafEntityImpl(id1), createOafEntityImpl(id2), createOafEntityImpl(id3), createOafEntityImpl(id4)
        );
        Dataset<OafEntityImpl> entityDS = spark.createDataset(entityData, Encoders.bean(OafEntityImpl.class));

        List<String> actionPayloadData = Arrays.asList(
                actionPayload(id1),
                actionPayload(id2), actionPayload(id2),
                actionPayload(id3), actionPayload(id3), actionPayload(id3)
        );
        Dataset<String> actionPayloadDS = spark.createDataset(actionPayloadData, Encoders.STRING());

        BiFunction<Dataset<OafEntityImpl>, Dataset<String>, Column> entityToActionPayloadJoinExpr = (left, right) ->
                left.col("id").equalTo(get_json_object(right.col("value"), "$.id"));
        BiFunction<String, Class<OafEntityImpl>, OafEntityImpl> actionPayloadToEntityFn =
                (BiFunction<String, Class<OafEntityImpl>, OafEntityImpl> & Serializable) (s, clazz) -> {
                    try {
                        JsonNode jsonNode = new ObjectMapper().readTree(s);
                        String id = jsonNode.at("/id").asText();
                        OafEntityImpl x = new OafEntityImpl();
                        x.setId(id);
                        return x;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };

        // when
        List<OafEntityImpl> results = PromoteActionSetFromHDFSFunctions
                .joinEntitiesWithActionPayloadAndMerge(entityDS,
                        actionPayloadDS,
                        entityToActionPayloadJoinExpr,
                        actionPayloadToEntityFn,
                        OafEntityImpl.class)
                .collectAsList();
        System.out.println(results.stream().map(x -> String.format("%s:%d", x.getId(), x.merged)).collect(Collectors.joining(",")));

        // then
        assertEquals(7, results.size());
        results.forEach(result -> {
            switch (result.getId()) {
                case "id1":
                    assertEquals(2, result.merged);
                    break;
                case "id2":
                    assertEquals(2, result.merged);
                    break;
                case "id3":
                    assertEquals(2, result.merged);
                    break;
                case "id4":
                    assertEquals(1, result.merged);
                    break;
            }
        });
    }

    public static class OafEntityImpl extends OafEntity {
        private int merged = 1;

        @Override
        public void mergeFrom(OafEntity e) {
            merged += ((OafEntityImpl) e).merged;
        }

        public int getMerged() {
            return merged;
        }

        public void setMerged(int merged) {
            this.merged = merged;
        }
    }

    private static OafEntityImpl createOafEntityImpl(String id) {
        OafEntityImpl x = new OafEntityImpl();
        x.setId(id);
        return x;
    }

    private static String actionPayload(String id) {
        return String.format("{\"id\":\"%s\"}", id);
    }
}
