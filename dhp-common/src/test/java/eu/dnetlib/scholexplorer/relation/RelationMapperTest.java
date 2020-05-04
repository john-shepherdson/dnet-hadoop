
package eu.dnetlib.scholexplorer.relation;

import org.junit.jupiter.api.Test;

public class RelationMapperTest {

	@Test
	public void testLoadRels() throws Exception {

		RelationMapper relationMapper = RelationMapper.load();
		relationMapper.keySet().forEach(System.out::println);
	}
}
