/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter, Christoph Boden
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment4;

import com.google.common.collect.Maps;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/* Classify document based on the trained probability (Training.JAVA)
 * Count(word, document) is considering #total word in all documents to approximate TF-IDF
 * word is filtered with stop words
 * and Test data is filtered by stop words, too.
 * This classification is without uniform assumption of document count, 
 * i.e. Count(document) is considered  
 */

public class Classification {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> conditionalInput = env.readTextFile(Config
				.pathToConditionals());
		DataSource<String> sumInput = env.readTextFile(Config.pathToSums());
		DataSource<String> docCountsInput = env.readTextFile(Config
				.pathToDocCounts());

		DataSet<Tuple3<String, String, Long>> conditionals = conditionalInput
				.map(new ConditionalReader());
		DataSet<Tuple2<String, Long>> sums = sumInput
				.map(new LabelCountReader());
		DataSet<Tuple2<String, Long>> docCounts = docCountsInput
				.map(new LabelCountReader());

		// Classify test.tab
		DataSource<String> testData = env.readTextFile(Config.pathToTestSet());

		DataSet<Tuple3<String, String, Double>> classifiedDataPoints = testData
				.map(new Classifier())
				.withBroadcastSet(conditionals, "conditionals")
				.withBroadcastSet(sums, "sums")
				.withBroadcastSet(docCounts, "docCounts");

		classifiedDataPoints.writeAsCsv(Config.pathToOutput(), "\n", "\t",
				FileSystem.WriteMode.OVERWRITE);

		// Classify secretest.dat
		/*
		 * DataSource<String> secretTestData =
		 * env.readTextFile(Config.pathToSecretTestSet());
		 * 
		 * DataSet<Tuple3<String, String, Double>> classifiedSecretTest =
		 * secretTestData .map(new Classifier()) .withBroadcastSet(conditionals,
		 * "conditionals") .withBroadcastSet(sums, "sums")
		 * .withBroadcastSet(docCounts, "docCounts");
		 * 
		 * classifiedSecretTest.writeAsCsv(Config.pathToUpload(), "\n", "\t",
		 * FileSystem.WriteMode.OVERWRITE);
		 */

		env.execute();
	}

	public static class ConditionalReader implements
			MapFunction<String, Tuple3<String, String, Long>> {

		@Override
		public Tuple3<String, String, Long> map(String s) throws Exception {
			String[] elements = s.split("\t");
			return new Tuple3<String, String, Long>(elements[0], elements[1],
					Long.parseLong(elements[2]));
		}
	}

	public static class LabelCountReader implements
			MapFunction<String, Tuple2<String, Long>> {

		@Override
		public Tuple2<String, Long> map(String s) throws Exception {
			String[] elements = s.split("\t");
			return new Tuple2<String, Long>(elements[0],
					Long.parseLong(elements[1]));
		}
	}

	public static class Classifier extends
			RichMapFunction<String, Tuple3<String, String, Double>> {

		// Label, Word, Count
		private final Map<String, Map<String, Long>> wordCounts = Maps
				.newHashMap();
		// Label, Count
		private final Map<String, Long> wordSums = Maps.newHashMap();
		// Label, Documents Count
		private final Map<String, Long> docCounts = Maps.newHashMap();

		private Set<String> stopWords;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			ArrayList<Tuple2<String, Long>> wordSumsArray = (ArrayList) getRuntimeContext()
					.getBroadcastVariable("sums");
			ArrayList<Tuple3<String, String, Long>> wordCountsArray = (ArrayList) getRuntimeContext()
					.getBroadcastVariable("conditionals");
			ArrayList<Tuple2<String, Long>> docCountsArray = (ArrayList) getRuntimeContext()
					.getBroadcastVariable("docCounts");

			setHashMap(wordSumsArray, wordCountsArray, docCountsArray);
		}

		@Override
		public Tuple3<String, String, Double> map(String line) throws Exception {

			String[] tokens = line.split("\t");
			String label = tokens[0];
			String[] terms = tokens[1].split(",");

			double maxProbability = Double.NEGATIVE_INFINITY;
			String predictionLabel = "";

			// Compute prob for all labels and get the maximum probability
			for (Entry<String, Long> entry : wordSums.entrySet()) {
				String tempLabel = entry.getKey();
				double wordsum = (double) entry.getValue();
				double condProb[] = new double[terms.length];
				double posterior = 1;
				double allCondProb = 0;

				// Ignore #totalDocuments because all need to multiply it
				double prior = (double) docCounts.get(tempLabel);

				// Compute prob for all terms in this label
				condProb = getCondProb(terms, tempLabel, wordsum);

				for (double c : condProb)
					allCondProb += c;

				posterior = Math.log(prior) + allCondProb;
				// Uniform assumption's computation
				// posterior = 1 * allCondProb;

				if (posterior > maxProbability) {
					maxProbability = posterior;
					predictionLabel = tempLabel;
				}
			}

			return new Tuple3<String, String, Double>(label, predictionLabel,
					maxProbability);
		}

		public double[] getCondProb(String[] terms, String tempLabel,
				double wordSum) {

			// conProb = nomCondProb / divCondProb
			double nomCondProb = 0;
			double divCondProb = 0;
			double condProb[] = new double[terms.length];

			setStopWords();

			for (int i = 0; i < terms.length; i++) {
				String valuelowCase = terms[i].toLowerCase();
				// remove punctuation
				String regex = "[^a-z-']";
				String value = valuelowCase.replaceAll(regex, "");
				if (!stopWords.contains(value) && (!value.isEmpty())
						&& value.length() > 1) {
					if (wordCounts.get(tempLabel).containsKey(terms[i])) {
						// Count(w,c)
						nomCondProb = wordCounts.get(tempLabel).get(terms[i])
								+ Config.getSmoothingParameter();
					} else {
						nomCondProb = Config.getSmoothingParameter();
					}
					// Count(all w,c) + k
					divCondProb = (wordSum + Config.getSmoothingParameter());
					// (Count(w,c) + k) / (Count(all w,c) + k)
					condProb[i] = Math.log(nomCondProb / divCondProb);
				}
			}
			return condProb;
		}

		public void setHashMap(ArrayList<Tuple2<String, Long>> wordSumsArray,
				ArrayList<Tuple3<String, String, Long>> wordCountsArray,
				ArrayList<Tuple2<String, Long>> docCountsArray) {

			for (Tuple2<String, Long> s : wordSumsArray)
				wordSums.put(s.f0, s.f1);

			for (Tuple2<String, Long> d : docCountsArray)
				docCounts.put(d.f0, d.f1);

			for (Tuple3<String, String, Long> c : wordCountsArray) {
				if (!wordCounts.containsKey(c.f0)) {
					Map<String, Long> temp = Maps.newHashMap();
					wordCounts.put(c.f0, temp);
				}
				if (!wordCounts.get(c.f0).containsKey(c.f1)) {
					wordCounts.get(c.f0).put(c.f1, c.f2);
				} else
					System.out.println("wordCounts lost!");
			}
		}

		public void setStopWords() {
			stopWords = new HashSet<String>();

			String[] stopWordsArray = { "m", "a", "actually", "about", "above",
					"across", "article", "after", "afterwards", "again",
					"against", "all", "almost", "alone", "along", "already",
					"also", "although", "always", "am", "among", "amongst",
					"amoungst", "amount", "an", "and", "another", "any",
					"anyhow", "anyone", "anything", "anyway", "anywhere",
					"are", "around", "as", "at", "back", "bad", "big", "be",
					"became", "because", "become", "becomes", "becoming",
					"been", "before", "beforehand", "behind", "being", "below",
					"beside", "besides", "between", "beyond", "bill", "both",
					"bottom", "but", "by", "call", "can", "can't", "cannot",
					"cant", "co", "con", "could", "couldn't", "couldnt", "cry",
					"de", "describe", "detail", "do", "done", "doesn't",
					"don't", "doing", "did", "didn't", "down", "due", "during",
					"each", "eg", "eight", "either", "eleven", "else",
					"elsewhere", "empty", "enough", "etc", "even", "ever",
					"every", "everyone", "everything", "everywhere", "except",
					"few", "fifteen", "fify", "fill", "find", "fire", "first",
					"five", "for", "former", "formerly", "forty", "found",
					"four", "from", "front", "full", "further", "get", "give",
					"good", "getting", "going", "great", "go", "had", "has",
					"hasnt", "have", "he", "hence", "her", "here", "hereafter",
					"hereby", "herein", "hereupon", "hers", "herself", "him",
					"himself", "his", "how", "however", "hundred", "ie", "if",
					"in", "inc", "indeed", "interest", "into", "i", "i'd",
					"important", "i'll", "i'm", "i've", "isn't", "it's", "is",
					"it", "its", "itself", "just", "know", "keep", "last",
					"latter", "latterly", "least", "less", "ltd", "like",
					"made", "many", "may", "me", "meanwhile", "might", "mill",
					"mine", "more", "new", "moreover", "most", "mostly",
					"move", "much", "must", "my", "mail", "make", "myself",
					"name", "namely", "neither", "never", "nice",
					"nevertheless", "next", "nine", "no", "nobody", "none",
					"noone", "nor", "not", "nothing", "now", "nowhere", "of",
					"off", "often", "on", "once", "one", "only", "onto", "or",
					"other", "others", "otherwise", "our", "ours", "ourselves",
					"out", "over", "own", "part", "per", "perhaps", "please",
					"pretty", "problem", "people", "put", "questions",
					"rather", "re", "really", "read", "subject", "same", "see",
					"sure", "seem", "seemed", "seeming", "seems", "serious",
					"said", "says", "several", "she", "should", "show", "side",
					"since", "sincere", "six", "sixty", "so", "some", "say",
					"somehow", "someone", "something", "sometime", "sometimes",
					"seen", "somewhere", "still", "such", "system", "take",
					"ten", "think", "than", "that", "that's", "the", "their",
					"them", "time", "there's", "themselves", "then", "thence",
					"there", "thereafter", "thereby", "therefore", "therein",
					"thereupon", "these", "things", "thing", "thought",
					"thanks", "tell", "they", "thickv", "thin", "third",
					"this", "those", "though", "three", "through",
					"throughout", "thru", "thus", "to", "together", "too",
					"top", "toward", "towards", "twelve", "twenty", "two",
					"use", "used", "un", "under", "until", "up", "upon",
					"writes", "us", "very", "via", "was", "we", "well", "were",
					"what", "want", "whatever", "when", "whence", "went",
					"whenever", "where", "we're", "whereafter", "whereas",
					"whereby", "wherein", "whereupon", "wherever", "whether",
					"which", "while", "whither", "who", "whoever", "whole",
					"whom", "whose", "why", "will", "with", "within",
					"without", "would", "wouldn't", "yet", "you", "yes",
					"your", "yours", "yourself", "yourselves", "you're",
					"year", "years", "you'll ", "you've ", "the" };

			stopWords = new HashSet<String>(stopWordsArray.length);

			for (String stopWord : stopWordsArray) {
				stopWords.add(stopWord);
			}
		}
	}
}
