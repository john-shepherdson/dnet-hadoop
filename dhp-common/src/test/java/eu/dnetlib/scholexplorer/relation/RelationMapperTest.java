
package eu.dnetlib.scholexplorer.relation;

import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class RelationMapperTest {

	@Test
	void testLoadRels() throws Exception {

		RelationMapper relationMapper = RelationMapper.load();
		assertFalse(relationMapper.isEmpty());
	}
}
