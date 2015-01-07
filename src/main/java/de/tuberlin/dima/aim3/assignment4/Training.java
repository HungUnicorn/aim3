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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import com.google.common.collect.Maps;
/* Generates the counts for each word belongs each document, count(word, document)
 Count(document) for P(document ), Count(total word) to weighted the count(word, document).
 Making counts(word, document) be similar to TF-IDF by divide with #total words.
 Ex, low #total words has higher count.
 Besides, filtering stop words and words with extreme low probability to accelerate efficiency*/

public class Training {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(Config.pathToTrainingSet());

		// read input with df-cut
		DataSet<Tuple3<String, String, Long>> labeledTerms = input
				.flatMap(new DataReader());
		
		DataSet<Tuple2<String, Long>> labeledDocuments = input
				.flatMap(new DocumentReader());

		// conditional counter per word per label
		DataSet<Tuple3<String, String, Long>> termCounts = labeledTerms
				.groupBy(0, 1).sum(2);

		// Word counts in all documents
		DataSet<Tuple2<String, Long>> wordSumDoc = termCounts.groupBy(1).sum(2)
				.project(1, 2).types(String.class, Long.class);

		// Word counts per word per label considering #words in all documents
		DataSet<Tuple3<String, String, Long>> weightTerm = termCounts
				.map(new WeightTerm())
				.withBroadcastSet(wordSumDoc, "wordSumDoc")
				.filter(new TermFilter());

		weightTerm.writeAsCsv(Config.pathToConditionals(), "\n", "\t",
				FileSystem.WriteMode.OVERWRITE);

		// Word counts per label considering #words in all documents
		DataSet<Tuple2<String, Long>> termLabelCounts = weightTerm.groupBy(0)
				.sum(2).project(0, 2).types(String.class, Long.class);

		termLabelCounts.writeAsCsv(Config.pathToSums(), "\n", "\t",
				FileSystem.WriteMode.OVERWRITE);

		// Document counts per label
		DataSet<Tuple2<String, Long>> docCounts = labeledDocuments.groupBy(0)
				.sum(1);

		docCounts.writeAsCsv(Config.pathToDocCounts(), "\n", "\t",
				FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	public static class DataReader implements
			FlatMapFunction<String, Tuple3<String, String, Long>> {
		
		HashSet<String> stopWords;

		@Override
		public void flatMap(String line,
				Collector<Tuple3<String, String, Long>> collector)
				throws Exception {
			
			String[] tokens = line.split("\t");
			String label = tokens[0];
			String[] terms = tokens[1].split(",");

			setStopWords();
			
			for (String term : terms) {
				String valuelowCase = term.toLowerCase();
				// Remove punctuation
				String regex = "[^a-z-']";
				String value = valuelowCase.replaceAll(regex, "");

				if (!stopWords.contains(value) && (!value.isEmpty())
						&& value.length() > 1) {
					collector.collect(new Tuple3<String, String, Long>(label,
							value, 1L));
				}
			}
		}

		public void setStopWords() {
			String[] s_stopWords = { "m", "a", "actually", "about", "above",
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

			stopWords = new HashSet<String>(s_stopWords.length);

			for (String stopWord : s_stopWords) {
				stopWords.add(stopWord);
			}
		}
	}

	public static class DocumentReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {
		@Override
		public void flatMap(String line,
				Collector<Tuple2<String, Long>> collector) throws Exception {

			String[] tokens = line.split("\t");
			String label = tokens[0];
			collector.collect(new Tuple2<String, Long>(label, 1L));
		}
	}

	// Filter meaningless terms to improve efficiency
	public static class TermFilter implements
			FilterFunction<Tuple3<String, String, Long>> {

		@Override
		public boolean filter(Tuple3<String, String, Long> value)
				throws Exception {
			return (value.f2 > Config.getSmoothingParameter());

		}
	}

	public static class WeightTerm
			extends
			RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>> {

		private final Map<String, Long> wordSumDocMap = Maps.newHashMap();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			ArrayList<Tuple2<String, Long>> wordSumDocArr = (ArrayList) getRuntimeContext()
					.getBroadcastVariable("wordSumDoc");

			for (Tuple2<String, Long> w : wordSumDocArr)
				wordSumDocMap.put(w.f0, w.f1);
		}

		@Override
		public Tuple3<String, String, Long> map(
				Tuple3<String, String, Long> value) throws Exception {
			double count = value.f2;
			double weightCount = 100 * count / wordSumDocMap.get(value.f1);
			Double.doubleToLongBits(weightCount);
			return new Tuple3<String, String, Long>(value.f0, value.f1,
					(long) weightCount);
		}
	}
}