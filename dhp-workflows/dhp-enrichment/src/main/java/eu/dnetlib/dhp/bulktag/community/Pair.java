
package eu.dnetlib.dhp.bulktag.community;

import com.google.gson.Gson;

import java.io.Serializable;

/** Created by miriam on 03/08/2018. */
public class Pair<A, B> implements Serializable {
	private A fst;
	private B snd;

	public A getFst() {
		return fst;
	}

	public Pair setFst(A fst) {
		this.fst = fst;
		return this;
	}

	public B getSnd() {
		return snd;
	}

	public Pair setSnd(B snd) {
		this.snd = snd;
		return this;
	}

	public Pair(A a, B b) {
		fst = a;
		snd = b;
	}

	public String toJson() {
		return new Gson().toJson(this);
	}
}
