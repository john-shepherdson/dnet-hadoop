
package eu.dnetlib.dhp.entitytoorganizationfromsemrel;

import java.io.Serializable;

import org.apache.spark.util.LongAccumulator;

public class PropagationCounter implements Serializable {
	private LongAccumulator iterationOne;
	private LongAccumulator iterationTwo;
	private LongAccumulator iterationThree;
	private LongAccumulator iterationFour;
	private LongAccumulator iterationFive;
	private LongAccumulator notReachedFirstParent;

	public PropagationCounter() {
	}

	public PropagationCounter(LongAccumulator iterationOne, LongAccumulator iterationTwo,
		LongAccumulator iterationThree, LongAccumulator iterationFour, LongAccumulator iterationFive,
		LongAccumulator notReachedFirstParent) {
		this.iterationOne = iterationOne;
		this.iterationTwo = iterationTwo;
		this.iterationThree = iterationThree;
		this.iterationFour = iterationFour;
		this.iterationFive = iterationFive;
		this.notReachedFirstParent = notReachedFirstParent;
	}

	public LongAccumulator getIterationOne() {
		return iterationOne;
	}

	public void setIterationOne(LongAccumulator iterationOne) {
		this.iterationOne = iterationOne;
	}

	public LongAccumulator getIterationTwo() {
		return iterationTwo;
	}

	public void setIterationTwo(LongAccumulator iterationTwo) {
		this.iterationTwo = iterationTwo;
	}

	public LongAccumulator getIterationThree() {
		return iterationThree;
	}

	public void setIterationThree(LongAccumulator iterationThree) {
		this.iterationThree = iterationThree;
	}

	public LongAccumulator getIterationFour() {
		return iterationFour;
	}

	public void setIterationFour(LongAccumulator iterationFour) {
		this.iterationFour = iterationFour;
	}

	public LongAccumulator getIterationFive() {
		return iterationFive;
	}

	public void setIterationFive(LongAccumulator iterationFive) {
		this.iterationFive = iterationFive;
	}

	public LongAccumulator getNotReachedFirstParent() {
		return notReachedFirstParent;
	}

	public void setNotReachedFirstParent(LongAccumulator notReachedFirstParent) {
		this.notReachedFirstParent = notReachedFirstParent;
	}
}
