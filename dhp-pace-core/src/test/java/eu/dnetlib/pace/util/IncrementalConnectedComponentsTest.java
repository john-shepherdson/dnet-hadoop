
package eu.dnetlib.pace.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class IncrementalConnectedComponentsTest {

	@Test
	public void transitiveClosureTest() {
		IncrementalConnectedComponents icc = new IncrementalConnectedComponents(10);

		icc.connect(0, 1);
		icc.connect(0, 2);
		icc.connect(0, 3);

		icc.connect(1, 2);
		icc.connect(1, 4);
		icc.connect(1, 5);

		icc.connect(6, 7);
		icc.connect(6, 9);

		assertEquals(icc.getConnections(0).toString(), "{0, 1, 2, 3, 4, 5}");
		assertEquals(icc.getConnections(1).toString(), "{0, 1, 2, 3, 4, 5}");
		assertEquals(icc.getConnections(2).toString(), "{0, 1, 2, 3, 4, 5}");
		assertEquals(icc.getConnections(3).toString(), "{0, 1, 2, 3, 4, 5}");
		assertEquals(icc.getConnections(4).toString(), "{0, 1, 2, 3, 4, 5}");
		assertEquals(icc.getConnections(5).toString(), "{0, 1, 2, 3, 4, 5}");

		assertEquals(icc.getConnections(6).toString(), "{6, 7, 9}");
		assertEquals(icc.getConnections(7).toString(), "{6, 7, 9}");
		assertEquals(icc.getConnections(9).toString(), "{6, 7, 9}");

		assertNull(icc.getConnections(8));
	}

}
