/*
 * Copyright 2019 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.utility;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author gregorygraham
 */
public class SearchString {

	private String searchString;

	public SearchString(String searchTerms) {
		this.searchString = searchTerms;
	}

	private static final Pattern QUOTED_STR = java.util.regex.Pattern.compile("\\\"[^\\\"]*\\\"");
	private static final Pattern QUOTE_AT_START = java.util.regex.Pattern.compile("^\"\"");
	private static final Pattern QUOTE_AT_END = java.util.regex.Pattern.compile("\"$");
	private static final Pattern QUOTE_ANY = java.util.regex.Pattern.compile("\"");

	public SearchString add(String string) {
		if (searchString.isEmpty()) {
			this.searchString += string;
		} else {
			this.searchString += " " + string;
		}
		return this;
	}

	public SearchString addQuotedTerm(String string) {
		return add("\"" + string + "\"");
	}

	public SearchString addPreferredTerm(String string) {
		for (String str : string.split(" ")) {
			add("+" + str);
		}
		return this;
	}

	public SearchString addReducedTerm(String string) {
		for (String str : string.split(" ")) {
			add("-" + str);
		}
		return this;
	}

	public Term[] getSearchTerms() throws NothingToSearchFor {
		List<Term> results = new ArrayList<>(0);
		SeparatedString separated = new SeparatedString(" ");
		if (searchString != null) {
			String replaced = searchString;
			Matcher matcher;
			do {
				matcher = QUOTED_STR.matcher(replaced);
				if (matcher.find()) {
					String group = matcher.group();
					String termString = QUOTE_AT_START.matcher(group).replaceAll("");
					termString = QUOTE_AT_END.matcher(termString).replaceAll("");
					results.add(new Term(termString, CONTAINS_QUOTED_SEARCH_WORD_VALUE));
					separated.add(termString);
					replaced = matcher.replaceFirst("");
					matcher = QUOTED_STR.matcher(replaced);
				}
			} while (matcher.find());
			replaced = QUOTE_ANY.matcher(replaced).replaceAll("");
			String[] split = replaced.split(" ");
			for (String string : split) {
				if (!string.isEmpty()) {
					if (string.startsWith("+")) {
						final String termString = string.replace("+", "");
						results.add(new Term(termString, CONTAINS_WANTED_SEARCH_WORD_VALUE));
						separated.add(termString);
					} else if (string.startsWith("-")) {
						final String termString = string.replace("-", "");
						results.add(new Term(termString, CONTAINS_UNWANTED_SEARCH_WORD_VALUE));
						separated.add(termString);
					} else {
						final String termString = string;
						results.add(new Term(termString,CONTAINS_SEARCH_WORD_VALUE));
						separated.add(termString);
					}
				}
			}
			results.add(new Term(separated.toString(), CONTAINS_EXACT_MATCH_VALUE));
			return results.toArray(new Term[]{});
		} else {
			throw new NothingToSearchFor();
		}
	}
	final static int CONTAINS_EXACT_MATCH_VALUE = 10000;
	final static int CONTAINS_INSENSITIVE_MATCH_VALUE = 1000;
	final static int CONTAINS_QUOTED_SEARCH_WORD_VALUE = 100;
	final static int CONTAINS_WANTED_SEARCH_WORD_VALUE = 100;
	final static int CONTAINS_SEARCH_WORD_VALUE = 10;
	final static int CONTAINS_UNWANTED_SEARCH_WORD_VALUE = -2;

	public static class NothingToSearchFor extends Exception {

		public NothingToSearchFor() {
		}
	}

	public static class Term {

		private final int value;
		private final String string;

		public Term(String string) {
			this(string,CONTAINS_SEARCH_WORD_VALUE);
		}
		public Term(String string, int value) {
			this.string = string;
			this.value = value;
		}

		public String getString() {
			return string;
		}

		public int getValue() {
			return value;
		}
	}
}
