
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class TestZenodoConnection {

	@Test
	public void test1() throws IOException {
		APIClient s = new APIClient(
			"https://sandbox.zenodo.org/api/deposit/depositions?access_token=5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe");

	}
}
