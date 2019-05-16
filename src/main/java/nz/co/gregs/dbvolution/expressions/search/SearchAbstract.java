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
package nz.co.gregs.dbvolution.expressions.search;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import nz.co.gregs.dbvolution.expressions.AnyExpression;
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.ExpressionHasStandardStringResult;
import nz.co.gregs.dbvolution.utility.SeparatedString;

/**
 *
 * @author gregorygraham
 */
public abstract class SearchAbstract {

	static final int CONTAINS_EXACT_MATCH_VALUE = 10000;
	static final int CONTAINS_INSENSITIVE_MATCH_VALUE = 1000;
	static final int CONTAINS_QUOTED_SEARCH_WORD_VALUE = 100;
	static final int CONTAINS_WANTED_SEARCH_WORD_VALUE = 100;
	static final int CONTAINS_SEARCH_WORD_VALUE = 10;
	static final int CONTAINS_UNWANTED_SEARCH_WORD_VALUE = -2;

	protected String searchString;

	public SearchAbstract() {
	}

	public SearchAbstract(String search) {
		this();
		setSearchString(search);
	}

	protected void setSearchString(String search) {
		this.searchString = search;
	}

	public SearchAbstract add(String string) {
		if (getSearchString().isEmpty()) {
			this.searchString += string;
		} else {
			this.searchString += " " + string;
		}
		return this;
	}

	public SearchAbstract addQuotedTerm(String string) {
		return add("\"" + string + "\"");
	}

	public SearchAbstract addPreferredTerm(String string) {
		for (String str : string.split(" ")) {
			add("+" + str);
		}
		return this;
	}

	public SearchAbstract addReducedTerm(String string) {
		for (String str : string.split(" ")) {
			add("-" + str);
		}
		return this;
	}

	protected final Term[] getSearchTerms() throws NothingToSearchFor {
		List<Term> results = new ArrayList<>(0);
		SeparatedString separated = SeparatedString.bySpaces();
		String replaced = getSearchString();
		if (replaced != null) {
			replaced = processAliasedTerms(replaced, results, separated);
			replaced = processQuotedTerms(replaced, results, separated);
			separated.addAll(processUnquotedTerms(replaced, results));
			final String newTerm = separated.toString();
			try {
				results.add(new Term(newTerm, CONTAINS_EXACT_MATCH_VALUE));
			} catch (InvalidTermException ex) {
				//skip it
			}
			return results.toArray(new Term[]{});
		} else {
			throw new NothingToSearchFor();
		}
	}

	private String processAliasedTerms(String input, List<Term> results, SeparatedString separated) {
		String replaced = input;
		replaced = processQuotedAliasedTerms(replaced, results);
		replaced = processSimpleAliasedTerms(replaced, results);

		return replaced;
	}

	private static final Pattern ALIASED_STR = java.util.regex.Pattern.compile("(\\w*):(\\w*)");
	private static final Pattern ALIASED_QUOTED_STR = java.util.regex.Pattern.compile("(\\w*):\"([^\"]*)\"");

	private String processSimpleAliasedTerms(String input, List<Term> results) {
		String replaced = input;
		Matcher matcher = ALIASED_STR.matcher(replaced);
		while (matcher.find()) {
			String wholeMatch = matcher.group();
			String searchAlias = matcher.group(1);
			String searchTerm = matcher.group(2);
			if (!searchAlias.isEmpty() && !searchTerm.isEmpty()) {
				ArrayList<Term> arrayList = new ArrayList<Term>();
				processUnquotedTerms(searchTerm, arrayList);
				for (Term term : arrayList) {
					results.add(new Term(term.getString(), term.getValue(), searchAlias));
				}
			}
			replaced = replaced.replace(wholeMatch, "");
			matcher = ALIASED_STR.matcher(replaced);
		}
		return replaced;
	}

	private String processQuotedAliasedTerms(String input, List<Term> results) {
		String replaced = input;
		Matcher matcher = ALIASED_QUOTED_STR.matcher(replaced);
		while (matcher.find()) {
			String wholeMatch = matcher.group();
			String searchAlias = matcher.group(1);
			String searchTerm = matcher.group(2);
			if (!searchAlias.isEmpty() && !searchTerm.isEmpty()) {
				ArrayList<Term> arrayList = new ArrayList<Term>();
				processUnquotedTerms(searchTerm, arrayList);
				for (Term term : arrayList) {
					results.add(new Term(term.getString(), term.getValue(), searchAlias));
				}
			}
			replaced = replaced.replace(wholeMatch, "");
			matcher = ALIASED_QUOTED_STR.matcher(replaced);
		}
		return replaced;
	}

	protected IntegerExpression getRankingExpression(ExpressionAlias col) {
		final AnyExpression<?,?,?> column = col.getExpr();
		if (column instanceof ExpressionHasStandardStringResult) {
			StringExpression stringExpression = ((ExpressionHasStandardStringResult) column).stringResult();
			try {
				IntegerExpression expr = new IntegerExpression(0);
				final Term[] searchTerms = this.getSearchTerms();
				for (SearchAcross.Term term : searchTerms) {
					IntegerExpression newExpr = getRankingExpressionForTerm(stringExpression, term, col.getAlias());
					expr = expr.plus(newExpr);
				}
				return expr;
			} catch (NothingToSearchFor ex) {
				return IntegerExpression.value(-1);
			}
		}
		return IntegerExpression.value(-1);
	}

	protected List<String> processUnquotedTerms(String replaced, List<Term> results) {
		SeparatedString separated = SeparatedString.bySpaces();
		String[] split = replaced.split(" ");
		for (String string : split) {
			if (!string.isEmpty()) {
				if (string.startsWith("+")) {
					final String termString = string.replace("+", "");
					try {
						results.add(new Term(termString, CONTAINS_WANTED_SEARCH_WORD_VALUE));
						separated.add(termString);
					} catch (InvalidTermException ex) {
						//skip it
					}
				} else if (string.startsWith("-")) {
					final String termString = string.replace("-", "");
					try {
						results.add(new Term(termString, CONTAINS_UNWANTED_SEARCH_WORD_VALUE));
//					separated.add(termString);		
					} catch (InvalidTermException ex) {
						//skip it
					}
				} else {
					final String termString = string;
					try {
						results.add(new Term(termString, CONTAINS_SEARCH_WORD_VALUE));
						separated.add(termString);
					} catch (InvalidTermException ex) {
						//skip it
					}
				}
			}
		}
		return separated.getStrings();
	}

	private static final Pattern QUOTED_STR = Pattern.compile("\\\"[^\\\"]*\\\"");
	private static final Pattern QUOTE_AT_START = Pattern.compile("^\"");
	private static final Pattern QUOTE_AT_END = Pattern.compile("\"$");
	private static final Pattern QUOTE_ANY = Pattern.compile("\"");

	protected String processQuotedTerms(String input, List<Term> results, SeparatedString separated) {
		Matcher matcher;
		String replaced = input;
		do {
			matcher = QUOTED_STR.matcher(replaced);
			if (matcher.find()) {
				String group = matcher.group();
				String termString = QUOTE_AT_START.matcher(group).replaceAll("");
				termString = QUOTE_AT_END.matcher(termString).replaceAll("");
				try {
					results.add(new Term(termString, CONTAINS_QUOTED_SEARCH_WORD_VALUE));
					separated.add(termString);
				} catch (InvalidTermException ex) {
					//skip it
				}
				replaced = matcher.replaceFirst("");
				matcher = QUOTED_STR.matcher(replaced);
			}
		} while (matcher.find());
		replaced = QUOTE_ANY.matcher(replaced).replaceAll("");
		return replaced;
	}

	protected IntegerExpression getRankingExpressionForTerm(StringExpression stringExpression, Term term, String columnAlias) {
		if (term.hasString()
				&& (term.hasNoAlias() || term.getAlias().equalsIgnoreCase(columnAlias))) {
			IntegerExpression newExpr
					= // the term exactly is worth the normal value
					stringExpression.contains(term.getString()).ifThenElse(term.getValue(), 0);
			// exactly as a word is worth twice the value
			newExpr = newExpr.plus(stringExpression.contains(" " + term.getString() + " ").ifThenElse(term.getValue() * 2, 0));
			// as a case-insensitive word is worth the normal value
			newExpr = newExpr.plus(stringExpression.containsIgnoreCase(" " + term.getString() + " ").ifThenElse(term.getValue(), 0));
			// as a case-insensitive sequence is worth half the value
			newExpr = newExpr.plus(stringExpression.containsIgnoreCase(term.getString()).ifThenElse(term.getValue() / 2, 0));
			return newExpr;
		} else {
			return new IntegerExpression(0);
		}
	}

	/**
	 * @return the searchString
	 */
	protected String getSearchString() {
		return searchString;
	}

	public SearchAbstract addAliasedTerm(String string, String alias) {
		add(alias + ":" + string);
		return this;
	}

	public static class NothingToSearchFor extends Exception {

		private final static long serialVersionUID = 1l;

		public NothingToSearchFor() {
		}
	}

	public static class Term {

		private final int value;
		private final String string;
		private String alias = EMPTY_ALIAS;

		public static final String EMPTY_ALIAS = "";

		public Term(String string) {
			this(string, CONTAINS_SEARCH_WORD_VALUE, EMPTY_ALIAS);
		}

		public Term(String string, int value) throws InvalidTermException {
			this(string, value, EMPTY_ALIAS);
		}

		public Term(String string, int value, String alias) {
			this.string = string;
			this.value = value;
			this.alias = alias;
		}

		public String getString() {
			return string;
		}

		public int getValue() {
			return value;
		}

		public boolean hasNoAlias() {
			return alias.equals(EMPTY_ALIAS);
		}

		public final boolean hasAlias() {
			return !hasNoAlias();
		}

		private static final Pattern WHITESPACE = Pattern.compile("\\s");

		public boolean hasNoString() {
			return isInvalidTerm(string);
		}

		public static boolean isValidTerm(String termString) {
			return !isInvalidTerm(termString);
		}

		public static boolean isInvalidTerm(String termString) {
			return WHITESPACE.matcher(termString).replaceAll("").isEmpty();
		}

		public final boolean hasString() {
			return !hasNoString();
		}

		public String getAlias() {
			return alias;
		}
	}

	private static class InvalidTermException extends Exception {
    static final long serialVersionUID = -3387516993124229948L;

		public InvalidTermException() {
		}
	}
}
