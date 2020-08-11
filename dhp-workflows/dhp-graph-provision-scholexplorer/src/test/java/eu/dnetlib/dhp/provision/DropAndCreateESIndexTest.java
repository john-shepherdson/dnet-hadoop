
package eu.dnetlib.dhp.provision;

import org.junit.jupiter.api.Test;

public class DropAndCreateESIndexTest {

	public void testDropAndCreate() throws Exception {
		DropAndCreateESIndex.main("-c localhost -i dli_shadow".split(" "));

	}

}
