
package eu.dnetlib.scholexplorer.relation;

import org.junit.jupiter.api.Test;

class RelationMapperTest {

	@Test
	void testLoadRels() throws Exception {

		RelationMapper relationMapper = RelationMapper.load();
		relationMapper.keySet().forEach(System.out::println);
	}
}
