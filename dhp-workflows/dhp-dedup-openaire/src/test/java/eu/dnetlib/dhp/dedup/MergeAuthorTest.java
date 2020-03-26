package eu.dnetlib.dhp.dedup;

import eu.dnetlib.dhp.schema.oaf.Publication;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MergeAuthorTest {

    private List<Publication> publicationsToMerge;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    public void setUp() throws Exception {
        final String json = IOUtils.toString(this.getClass().getResourceAsStream("/eu/dnetlib/dhp/dedup/json/authors_merge.json"));


        publicationsToMerge = Arrays.asList(json.split("\n")).stream().map(s-> {
            try {
                return mapper.readValue(s, Publication.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    //FIX ME Michele DB this tests doesn't work
    //@Test
    public void test() throws  Exception {
        Publication dedup = new Publication();

        publicationsToMerge.forEach(p-> {
            dedup.mergeFrom(p);
            dedup.setAuthor(DedupUtility.mergeAuthor(dedup.getAuthor(),p.getAuthor()));
        });

        System.out.println(mapper.writeValueAsString(dedup));
    }

}
