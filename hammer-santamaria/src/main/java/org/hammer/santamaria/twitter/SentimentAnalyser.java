package org.hammer.santamaria.twitter;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datumbox.framework.applications.nlp.TextClassifier;
import com.datumbox.framework.common.Configuration;
import com.datumbox.framework.common.dataobjects.Record;
import com.datumbox.framework.common.utilities.PHPMethods;
import com.datumbox.framework.core.machinelearning.classification.MultinomialNaiveBayes;
import com.datumbox.framework.core.machinelearning.common.interfaces.ValidationMetrics;
import com.datumbox.framework.core.machinelearning.featureselection.categorical.ChisquareSelect;
import com.datumbox.framework.core.utilities.text.extractors.NgramsExtractor;

import twitter4j.Status;

public class SentimentAnalyser {

	/**
	 * LOG 
	 */
	private static final Log LOG = LogFactory.getLog(SentimentAnalyser.class);
	
	/**
	 * Text classifier 
	 */
	private TextClassifier classifier = null;

	/**
	 * Holder
	 */
	private static class SentimentAnalyserHolder {
		private final static SentimentAnalyser instance = new SentimentAnalyser();
	}

	/**
	 * Get Sentiment Analyser instance
	 * @return
	 */
	public static SentimentAnalyser getInstance() {
		return SentimentAnalyserHolder.instance;
	}
	
	/**
	 * Create a
	 * Sentiment Analyser
	 * @throws  
	 * 
	 */
	private SentimentAnalyser() {
		try {
			Configuration conf = Configuration.getConfiguration();
			Map<Object, URI> datasets = new HashMap<Object, URI>();
			datasets.put("positive", SentimentAnalyser.class.getClassLoader().getResource("rt.pos").toURI());
			datasets.put("negative", SentimentAnalyser.class.getClassLoader().getResource("rt.neg").toURI());
			TextClassifier.TrainingParameters trainingParameters = new TextClassifier.TrainingParameters();

			// Classifier configuration
			trainingParameters.setModelerClass(MultinomialNaiveBayes.class);
			trainingParameters.setModelerTrainingParameters(new MultinomialNaiveBayes.TrainingParameters());

			// Set data transfomation configuration
			trainingParameters.setDataTransformerClass(null);
			trainingParameters.setDataTransformerTrainingParameters(null);

			// Set feature selection configuration
			trainingParameters.setFeatureSelectorClass(ChisquareSelect.class);
			trainingParameters.setFeatureSelectorTrainingParameters(new ChisquareSelect.TrainingParameters());

			// Set text extraction configuration
			trainingParameters.setTextExtractorClass(NgramsExtractor.class);
			trainingParameters.setTextExtractorParameters(new NgramsExtractor.Parameters());

			// Fit the classifier
			// ------------------
			classifier = new TextClassifier("SentimentAnalysis", conf);
			classifier.fit(datasets, trainingParameters);

			ValidationMetrics vm = classifier.validate(datasets);
			classifier.setValidationMetrics(vm); // store them in the model for
													// future reference
		} catch (Exception e) {
			LOG.error(e);
		}

	}
	
	public String AnalyseSentiment(Status tweet) {// Classify a single sentence
		// Classify a single sentence
		String sentence = tweet.getText();
		Record r = classifier.predict(sentence);

		LOG.info("Classifing sentence: \"" + sentence + "\"");
		LOG.info("Predicted class: " + r.getYPredicted());
		LOG.info("Probability: " + r.getYPredictedProbabilities().get(r.getYPredicted()));
		LOG.info("Classifier Statistics: " + PHPMethods.var_export(classifier.getValidationMetrics()));
		LOG.info("------------------------------------------");
		LOG.info("------------------------------------------");
		
		return (String) r.getYPredicted();
	}
}
