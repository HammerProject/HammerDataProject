package org.hammer.pinta.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.hammer.isabella.fuzzy.JaroWinkler;
import org.hammer.isabella.query.Keyword;
import org.hammer.pinta.utils.WordNetUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * 
 * Pinta output commiter
 * 
 * @author mauro.pelucchi@gmail.com
 * @project Hammer-Project - Pinta
 *
 */
public class PintaOutputCommiter extends OutputCommitter {

	private static final Log LOG = LogFactory.getLog(PintaOutputCommiter.class);

	private int inserted = 0;
	private int updated = 0;

	private final DBCollection collection;
	public static final String TEMP_DIR_NAME = "_MONGO_OUT_TEMP";

	public PintaOutputCommiter(final DBCollection collection) {
		this.collection = collection;
	}

	private void cleanupTemporaryFiles(final TaskAttemptContext taskContext) throws IOException {
		Path tempPath = getTaskAttemptPath(taskContext);
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			fs.delete(tempPath, true);
		} catch (IOException e) {
			LOG.error("Could not delete temporary file " + tempPath, e);
			LOG.error(e);
			throw e;
		}
	}

	@Override
	public void abortTask(final TaskAttemptContext taskContext) throws IOException {
		LOG.info("COLOMBO COMMITTER - Aborting task.");
		cleanupTemporaryFiles(taskContext);
	}

	@Override
	public void commitTask(final TaskAttemptContext taskContext) throws IOException {
		LOG.info("COLOMBO COMMITER - Committing task.");

		inserted = 0;
		Path tempFilePath = getTaskAttemptPath(taskContext);
		LOG.info("COLOMBO -  Committing from temporary file: " + tempFilePath.toString());

		FSDataInputStream inputStream = null;
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			inputStream = fs.open(tempFilePath);
		} catch (IOException e) {
			LOG.error("COLOMBO COMMITER  Could not open temporary file for committing", e);
			LOG.error(e);
			cleanupAfterCommit(inputStream, taskContext);

			throw e;
		}

		LOG.info("COLOMBO Clean collection " + collection.getFullName());

		collection.remove(new BasicDBObject());
		LOG.info("COLOMBO After clear : collection " + collection.count());

		inserted = 0;
		updated = 0;
		while (inputStream.available() > 0) {
			try {
				BSONWritable bw = new BSONWritable();
				bw.readFields(inputStream);
				BasicDBObject bo = new BasicDBObject(bw.getDoc().toMap());
				BasicDBObject searchQuery = new BasicDBObject().append("keyword", bo.get("keyword"));
				DBCursor c = collection.find(searchQuery);
				if (c.hasNext()) {
					DBObject obj = c.next();
					@SuppressWarnings("unchecked")
					ArrayList<DBObject> newList = (ArrayList<DBObject>) bo.get("documents");
					if (newList == null) {
						newList = new ArrayList<DBObject>();
					}
					@SuppressWarnings("unchecked")
					ArrayList<DBObject> oldList = (ArrayList<DBObject>) obj.get("documents");
					if (oldList != null) {
						newList.addAll(oldList);
					}
					bo.remove("documents");
					bo.put("documents", newList);
					collection.update(searchQuery, bo);
					updated++;
				} else {
					collection.insert(bo);
					inserted++;
				}
				c.close();
				taskContext.progress();

			} catch (Exception e) {
				LOG.error(e);
				LOG.error("PINTA COMMITTER: Error reading from temporary file", e);
			}
		}

		LOG.info("COLOMBO INSERT - DATA SET : " + inserted);
		LOG.info("UPDATE : " + updated);

		cleanupAfterCommit(inputStream, taskContext);
	}

	public static Comparator<Keyword> CMP = new Comparator<Keyword>() {
		public int compare(Keyword o1, Keyword o2) {
			return (o1.getSimilarity() < o2.getSimilarity()) ? 1 : ((o1.getSimilarity() > o2.getSimilarity()) ? -1 : 0);
		}
	};

	/**
	 * Calc similar terms with Jaro-Winkler
	 */
	public static void CalcSimTerms(Configuration conf) {
		LOG.info("----- CALC SIM TERMS ------");
		double thSim = Precision.round(Double.parseDouble(conf.get("thSim")), 2);
		int maxSim = Integer.parseInt(conf.get("maxSim"));

		HashMap<String, Keyword> index = GetMyIndex(conf);
		float size = index.size();
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			MongoCollection<Document> myIdx = db.getCollection(conf.get("index-table") + "");
			float c = 0.0f;
			for (String term : index.keySet()) {
				List<Keyword> similatirySet = new ArrayList<Keyword>();
				for (String s : index.keySet()) {
					if (!s.equals(term)) {
						double sim = JaroWinkler.Apply(term.toLowerCase(), s.toLowerCase());
						// avg the value of sim with the value of re
						// we want to give more importance to terms that are
						// more
						// representative of our index
						double re = index.get(s).getReScore();
						sim = (sim + re) / 2.0d;

						if (sim >= thSim) {

							Keyword k = index.get(s).clone();
							k.setSimilarity(sim);
							similatirySet.add(k);

						}
					}
				}
				BasicDBObject mostSim = new BasicDBObject();
				if (similatirySet.size() > 0) {
					similatirySet.sort(CMP);
					mostSim.append("term", similatirySet.get(0).getKeyword().toLowerCase());
					mostSim.append("re", similatirySet.get(0).getReScore());
					mostSim.append("sim", similatirySet.get(0).getSimilarity());
				}
				// we update the keyword with re and list of similarity terms
				try {
					BasicDBObject searchQuery = new BasicDBObject().append("keyword", term.toLowerCase());

					Document updateField = new Document();
					BasicDBList simTermsList = new BasicDBList();
					int countSim = 0;
					for (Keyword k : similatirySet) {
						BasicDBObject kObj = new BasicDBObject();
						kObj.append("term", k.getKeyword().toLowerCase());
						kObj.append("re", k.getReScore());
						kObj.append("sim", k.getSimilarity());
						simTermsList.add(kObj);
						if (countSim >= maxSim) {
							break;
						}
						countSim++;
					}
					updateField.put("re", index.get(term).getReScore());
					updateField.put("sim-terms", simTermsList);
					if (similatirySet.size() > 0) {
						updateField.put("most-sim", mostSim);
					}
					Document updateObj = new Document("$set", updateField);
					myIdx.findOneAndUpdate(searchQuery, updateObj);
					LOG.debug("save " + term);

				} catch (Exception e) {
					LOG.error(e);
					LOG.error("PINTA COMMITTER: Error reading from temporary file", e);
				}
				c++;
				if ((c % 1000 == 0) || (Precision.round((c / size) * 100f, 3) > 99)) {
					LOG.info(Precision.round((c / size) * 100f, 3) + " %");
				}
			}
		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
			LOG.error(ex.getMessage());
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

	}

	/**
	 * Calc synset with WordNet (only for English terms)
	 */
	public static void CalcSynset(Configuration conf) {
		LOG.info("----- CALC SYNSET        ------");

		HashMap<String, Keyword> index = GetMyIndex(conf);
		float size = index.size();
		MongoClient mongo = null;
		MongoDatabase db = null;
		try {
			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());
			MongoCollection<Document> myIdx = db.getCollection(conf.get("index-table") + "");
			float c = 0.0f;
			for (String term : index.keySet()) {
				List<Keyword> synSet = new ArrayList<Keyword>();
				Map<String, Double> mySynSet = WordNetUtils.MySynset(conf.get("wn-home") + "", term);

				for (String s : mySynSet.keySet()) {
					if (!s.toLowerCase().equals(term.toLowerCase()) /* && index.containsKey(s) */) {
						// a term of synset has the same re of the original term
						// but i use prior probability to adjust
						double re = index.get(term).getReScore();
						re = re * mySynSet.get(s);
						Keyword k = index.get(term).clone();
						k.setKeyword(s.toLowerCase());
						k.setSimilarity(re);
						synSet.add(k);
					}
				}
				BasicDBObject mostSyn = new BasicDBObject();
				if (synSet.size() > 0) {
					synSet.sort(CMP);
					mostSyn.append("term", synSet.get(0).getKeyword().toLowerCase());
					mostSyn.append("re", synSet.get(0).getReScore());
					mostSyn.append("sim", synSet.get(0).getSimilarity());
				}

				// we update the keyword with re and list of synset terms
				try {
					BasicDBObject searchQuery = new BasicDBObject().append("keyword", term.toLowerCase());

					Document updateField = new Document();
					BasicDBList simTermsList = new BasicDBList();
					for (Keyword k : synSet) {
						BasicDBObject kObj = new BasicDBObject();
						kObj.append("term", k.getKeyword().toLowerCase());
						kObj.append("re", k.getReScore());
						kObj.append("sim", k.getSimilarity());
						simTermsList.add(kObj);
					}

					updateField.put("re", index.get(term).getReScore());
					updateField.put("syn-set", simTermsList);
					if (synSet.size() > 0) {
						updateField.put("most-syn", mostSyn);
					}
					Document updateObj = new Document("$set", updateField);
					Document myObj = myIdx.findOneAndUpdate(searchQuery, updateObj);
					LOG.debug("save " + term);

					/* insert synset terms */
					for (String s : mySynSet.keySet()) {

						BasicDBObject tempSearch = new BasicDBObject().append("keyword", s.toLowerCase());
						FindIterable<Document> myDoc = myIdx.find(tempSearch);
						if (myDoc.iterator().hasNext()) {
							Document obj = myDoc.iterator().next();
							Document myUpdateObj = new Document();
							@SuppressWarnings("unchecked")
							ArrayList<Document> newList = (ArrayList<Document>) myObj.get("documents");
							if (newList == null) {
								newList = new ArrayList<Document>();
							}
							@SuppressWarnings("unchecked")
							ArrayList<Document> oldList = (ArrayList<Document>) obj.get("documents");
							if (oldList != null) {
								for(Document o: oldList) {
									if(!newList.contains(o)) {
										newList.add(o);
									}
								}
							}
							myUpdateObj.put("documents", newList);
							Document myUpdate = new Document("$set", obj);
							myIdx.findOneAndUpdate(tempSearch, myUpdate);
						} else {
							Document bo = new Document();
							double re = index.get(term).getReScore();
							re = re * mySynSet.get(s);
							
							bo.append("re", re);
							bo.append("syn-set", simTermsList);
							bo.append("keyword", s.toLowerCase());
							bo.append("documents", myObj.get("documents"));
							bo.append("last-update", (new Date()));

							bo.append("wn", "true");
							myIdx.insertOne(bo);
						}

					}

				} catch (Exception e) {
					LOG.error(e);
					LOG.error("PINTA COMMITTER: Error reading from temporary file", e);
				}
				c++;
				if ((c % 1000 == 0) || (Precision.round((c / size) * 100f, 3) > 99)) {
					LOG.info(Precision.round((c / size) * 100f, 3) + " %");
				}
			}
		} catch (Exception ex) {
			LOG.error(ex);
			ex.printStackTrace();
			LOG.error(ex.getMessage());
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}

	}

	/**
	 * Helper method to close an FSDataInputStream and clean up any files still
	 * left around from map/reduce tasks.
	 * 
	 * @param inputStream
	 *            the FSDataInputStream to close.
	 */
	private void cleanupAfterCommit(final FSDataInputStream inputStream, final TaskAttemptContext context)
			throws IOException {
		if (inputStream != null) {
			try {
				inputStream.close();
			} catch (IOException e) {
				LOG.error(e);
				LOG.error("COLOMBO COMMITER  - Could not close input stream", e);
				throw e;
			}
		}
		cleanupTemporaryFiles(context);
	}

	@Override
	public boolean needsTaskCommit(final TaskAttemptContext taskContext) throws IOException {
		try {
			FileSystem fs = FileSystem.get(taskContext.getConfiguration());
			// Commit is only necessary if there was any output.
			return fs.exists(getTaskAttemptPath(taskContext));
		} catch (IOException e) {
			LOG.error(e);
			LOG.error("COLOMBO COMMITER  - Could not open filesystem", e);
			throw e;
		}
	}

	@Override
	public void setupJob(final JobContext jobContext) {
		LOG.info("COLOMBO COMMITER - Setting up job.");
	}

	@Override
	public void setupTask(final TaskAttemptContext taskContext) {
		LOG.info("COLOMBO COMMITER - Setting up task.");
	}

	/**
	 * Get the Path to where temporary files should be stored for a TaskAttempt,
	 * whose TaskAttemptContext is provided.
	 *
	 * @param context
	 *            the TaskAttemptContext.
	 * @return the Path to the temporary file for the TaskAttempt.
	 */
	public static Path getTaskAttemptPath(final TaskAttemptContext context) {
		Configuration config = context.getConfiguration();
		// Try to use the following base temporary directories, in this order:
		// 1. New-style option for task tmp dir
		// 2. Old-style option for task tmp dir
		// 3. Hadoop system-wide tmp dir
		// 4. /tmp
		String basePath = config.get("mapreduce.task.tmp.dir",
				config.get("mapred.child.tmp", config.get("hadoop.tmp.dir", "/tmp")));
		// Hadoop Paths always use "/" as a directory separator.
		return new Path(String.format("%s/%s/%s/_out", basePath, context.getTaskAttemptID().toString(), TEMP_DIR_NAME));
	}

	/**
	 * Get the inverted index
	 * 
	 * @param conf
	 * @param q
	 * @return
	 */
	public static HashMap<String, Keyword> GetMyIndex(Configuration conf) {
		MongoClient mongo = null;
		final HashMap<String, Keyword> index = new HashMap<String, Keyword>();
		MongoDatabase db = null;
		LOG.debug("Select my index...");
		try {

			MongoClientURI inputURI = MongoConfigUtil.getInputURI(conf);
			mongo = new MongoClient(inputURI);
			db = mongo.getDatabase(inputURI.getDatabase());

			MongoCollection<Document> myIdx = db.getCollection(conf.get("index-table") + "");
			final long totalResources = myIdx.count();
			LOG.info("TOTAL INDEX KEYWORDS ---> " + totalResources);
			FindIterable<Document> iterable = myIdx.find();

			iterable.forEach(new Block<Document>() {

				@SuppressWarnings("unchecked")
				public void apply(final Document document) {
					ArrayList<Document> docList = (ArrayList<Document>) document.get("documents");
					long mT = docList.size();
					Keyword k = new Keyword(document.getString("keyword"), totalResources, mT);
					index.put(document.getString("keyword"), k);
				}
			});

		} catch (Exception ex) {
			LOG.debug(ex);
			LOG.error(ex.getMessage());
		} finally {
			if (mongo != null) {
				mongo.close();
			}
		}
		return index;

	}

}