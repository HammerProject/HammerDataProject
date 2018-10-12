package org.hammer.isabella.cc.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.hammer.isabella.cc.Isabella;
import org.hammer.isabella.cc.ParseException;
import org.hammer.isabella.query.IsabellaError;
import org.hammer.isabella.query.Keyword;
import org.hammer.isabella.query.QueryGraph;
import org.hammer.isabella.query.Term;

public class QueryGraphCloner {

	// so that nobody can accidentally create an ObjectCloner object
	private QueryGraphCloner() {
	}

	// returns a deep copy of an object
	static public QueryGraph deepCopy(String q, ArrayList<String[]> arrayList, HashMap<String, Keyword> index)
			throws Exception {

		for (String[] k : arrayList) {
			if (!k[0].equals("select") && !k[0].equals("where") && !k[0].equals("from") && !k[0].equals("label1")
					&& !k[0].equals("value") && !k[0].equals("instance1") && !k[0].equals("instance")
					&& !k[0].equals("label")) {
				q = q.replaceAll(k[0], k[1]);
			}
		}

		Isabella parser = new Isabella(new StringReader(q));
		QueryGraph query;

		try {
			query = parser.queryGraph();
		} catch (ParseException e) {
			throw new IOException(e);
		}
		query.setIndex(index);

		for (IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}

		if (parser.getErrors().size() > 0) {
			throw new IOException("Query syntax not correct.");
		}

		return query;
	}

	// returns a deep copy of an object
	static public QueryGraph deepCopyTerm(String q, List<Term[]> arrayList, HashMap<String, Keyword> index)
			throws Exception {

		for (Term[] k : arrayList) {
			if (!k[0].getTerm().equals("select") && !k[0].getTerm().equals("where") && !k[0].getTerm().equals("from")
					&& !k[0].getTerm().equals("label1") && !k[0].getTerm().equals("value")
					&& !k[0].getTerm().equals("instance1") && !k[0].getTerm().equals("instance")
					&& !k[0].getTerm().equals("label")) {
				q = q.replaceAll(k[0].getTerm(), k[1].getTerm());
			}
		}

		Isabella parser = new Isabella(new StringReader(q));
		QueryGraph query;

		try {
			query = parser.queryGraph();
		} catch (ParseException e) {
			throw new IOException(e);
		}
		query.setIndex(index);

		for (IsabellaError err : parser.getErrors().values()) {
			System.out.println(err.toString());
		}

		if (parser.getErrors().size() > 0) {
			throw new IOException("Query syntax not correct.");
		}

		return query;
	}

}
