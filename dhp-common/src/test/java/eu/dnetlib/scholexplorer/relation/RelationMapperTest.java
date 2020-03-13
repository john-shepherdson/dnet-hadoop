package eu.dnetlib.scholexplorer.relation;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class RelationMapperTest {

    @Test
    public void testLoadRels() throws Exception{

        RelationMapper relationMapper = RelationMapper.load();
        relationMapper.keySet().forEach(System.out::println);

    }
}
