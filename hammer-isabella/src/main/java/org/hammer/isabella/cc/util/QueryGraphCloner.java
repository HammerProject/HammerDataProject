package org.hammer.isabella.cc.util;

import org.hammer.isabella.query.QueryGraph;

import com.rits.cloning.Cloner;

public class QueryGraphCloner {

	// so that nobody can accidentally create an ObjectCloner object
	private QueryGraphCloner() {
	}

	// returns a deep copy of an object
	static public QueryGraph deepCopy(QueryGraph oldObj) throws Exception {
		Cloner cloner = new Cloner();

		return (QueryGraph) cloner.deepClone(oldObj);
		
		/*ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream(); // A
			oos = new ObjectOutputStream(bos); // B
			// serialize and pass the object
			oos.writeObject(oldObj); // C
			oos.flush(); // D
			ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray()); // E
			ois = new ObjectInputStream(bin); // F
			// return the new object
			return (QueryGraph) ois.readObject(); // G
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception in ObjectCloner = " + e);
			throw (e);
		} finally {
			oos.close();
			ois.close();
		}*/
	}

}
