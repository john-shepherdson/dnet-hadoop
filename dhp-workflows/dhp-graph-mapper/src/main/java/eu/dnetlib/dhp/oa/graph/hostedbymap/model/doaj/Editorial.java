
package eu.dnetlib.dhp.oa.graph.hostedbymap.model.doaj;

import java.io.Serializable;
import java.util.List;

public class Editorial implements Serializable {
	private List<String> review_process;
	private String review_url;
	private String board_url;

	public List<String> getReview_process() {
		return review_process;
	}

	public void setReview_process(List<String> review_process) {
		this.review_process = review_process;
	}

	public String getReview_url() {
		return review_url;
	}

	public void setReview_url(String review_url) {
		this.review_url = review_url;
	}

	public String getBoard_url() {
		return board_url;
	}

	public void setBoard_url(String board_url) {
		this.board_url = board_url;
	}
}
