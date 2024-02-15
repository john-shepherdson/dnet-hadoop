
package eu.dnetlib.pace.util;

import java.util.BitSet;

public class IncrementalConnectedComponents {
	final private int size;

	final private BitSet[] indexes;

	IncrementalConnectedComponents(int size) {
		this.size = size;
		this.indexes = new BitSet[size];
	}

	public void connect(int i, int j) {
		if (indexes[i] == null) {
			if (indexes[j] == null) {
				indexes[i] = new BitSet(size);
			} else {
				indexes[i] = indexes[j];
			}
		} else {
			if (indexes[j] != null && indexes[i] != indexes[j]) {
				// merge adjacency lists for i and j
				indexes[i].or(indexes[j]);
			}
		}

		indexes[i].set(i);
		indexes[i].set(j);
		indexes[j] = indexes[i];
	}

	public int nextUnconnected(int i, int j) {
		if (indexes[i] == null) {
			return j;
		}
		int result = indexes[i].nextClearBit(j);

		return (result >= size) ? -1 : result;
	}

	public BitSet getConnections(int i) {
		if (indexes[i] == null) {
			return null;
		}
		return indexes[i];
	}
}
