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
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.results.ExpressionHasStandardStringResult;

/**
 * Standardised searching using string terms and expression aliases.
 *
 * <p>
 * Designed to provide easy access to complex user-driven searching such as
 * 'terminator -schwarzenagger "come with me if" desc:quote author:+"james
 * cameron"'.</p>
 *
 * <p>
 * Search terms can be single words or sequence, or quoted phrases. Terms can
 * also be prioritized with + and - and restricted to a single column using an
 * alias followed by a colon (alias:term). Searching for any empty value can be
 * done with an alias followed by empty quotes, for example description:""</p>
 *
 * <p>
 * Use with a single column using {@link StringExpression#searchFor(nz.co.gregs.dbvolution.expressions.search.SearchString)
 * } and {@link StringExpression#searchForRanking(nz.co.gregs.dbvolution.expressions.search.SearchString)
 * }: marq.column(marq.name).searchFor(searchString). If you have individual
 * strings use
 * {@link StringExpression#searchFor(java.lang.String...) and {@link StringExpression#searchForRanking(java.lang.String...) }.</p>
 *
 * <p>
 * searchForRanking produces a number value that can be used for sorting. </p>
 *
 * @author gregorygraham
 */
public abstract class SearchAbstract {

	protected String searchString;

	private static final Pattern TERM_PATTERN = Pattern.compile("((\\w+):){0,1}([+-]{0,1})((\\w+)|(\"([^\"]*)\"?))");

	private static final int ALIAS_GROUP = 2;
	private static final int MODE_GROUP = 3;
	private static final int SIMPLE_TERM_GROUP = 4;
	private static final int QUOTED_TERM_GROUP = 7;

	public SearchAbstract() {
	}

	public SearchAbstract(String search) {
		this();
		setSearchString(search);
	}

	protected SearchAbstract setSearchString(String search) {
		this.searchString = search;
		return this;
	}

	public SearchAbstract addToSearchString(String string) {
		if (getSearchString().isEmpty()) {
			this.searchString += string;
		} else {
			this.searchString += " " + string;
		}
		return this;
	}

	public SearchAbstract addQuotedTermToSearchString(String string) {
		return addToSearchString("\"" + string + "\"");
	}

	public SearchAbstract addPreferredTermToSearchString(String string) {
		for (String str : string.split(" ")) {
			addToSearchString("+" + str);
		}
		return this;
	}

	public SearchAbstract addReducedTermToSearchString(String string) {
		for (String str : string.split(" ")) {
			addToSearchString("-" + str);
		}
		return this;
	}

	protected final Term[] getSearchTerms() {
		List<Term> terms = new ArrayList<>();
		Matcher matcher = TERM_PATTERN.matcher(getSearchString());
		while (matcher.find()) {
			String all = matcher.group(WHOLE_MATCH_GROUP);
			String alias = matcher.group(ALIAS_GROUP);
			String modeStr = matcher.group(MODE_GROUP);
			Mode mode = modeStr.equals("+") ? Mode.PLUS : modeStr.equals("-") ? Mode.MINUS : Mode.NORMAL;
			String simpleTerm = matcher.group(SIMPLE_TERM_GROUP);
			String quotedTerm = matcher.group(QUOTED_TERM_GROUP);
			if (quotedTerm != null) {
				terms.add(new Term(quotedTerm, true, mode, alias));
			} else {
				terms.add(new Term(simpleTerm, false, mode, alias));
			}
		}
		return terms.toArray(new Term[]{});
	}

	private static final int WHOLE_MATCH_GROUP = 0;

	protected final NumberExpression getRankingExpression(ExpressionAlias col) {
		final AnyExpression<?, ?, ?> column = col.getExpr();
		if (column instanceof ExpressionHasStandardStringResult) {
			StringExpression stringExpression = ((ExpressionHasStandardStringResult) column).stringResult();
			NumberExpression expr = new NumberExpression(0);
			final Term[] searchTerms = this.getSearchTerms();
			for (SearchAcross.Term term : searchTerms) {
				NumberExpression newExpr = getRankingExpressionForTerm(stringExpression, term, col.getAlias());
				expr = expr.plus(newExpr);
			}
			return expr;
		}
		return NumberExpression.value(-1.0);
	}

	protected final NumberExpression getRankingExpressionForTerm(StringExpression stringExpression, Term term, String columnAlias) {
		if (term.hasString()
				&& (term.hasNoAlias() || term.aliasMatches(columnAlias))) {
			NumberExpression newExpr
					= // the term exactly is worth the normal value
					stringExpression.contains(term.getString()).ifThenElse(term.getValue(), 0.0);
			// exactly as a word is worth twice the value
			newExpr = newExpr.plus(stringExpression.contains(" " + term.getString() + " ").ifThenElse(term.getValue() * 2, 0.0));
			// as a case-insensitive word is worth the normal value
			newExpr = newExpr.plus(stringExpression.containsIgnoreCase(" " + term.getString() + " ").ifThenElse(term.getValue(), 0.0));
			// as a case-insensitive sequence is worth half the value
			newExpr = newExpr.plus(stringExpression.containsIgnoreCase(term.getString()).ifThenElse(term.getValue() / 2, 0.0));
			return newExpr;
		} else if (term.hasAlias() && term.isQuoted() && term.hasNoString() && term.aliasMatches(columnAlias)) {
			return stringExpression.is("").ifThenElse(term.getValue(), 0);
		} else {
			return new NumberExpression(0);
		}
	}

	/**
	 * @return the searchString
	 */
	protected final String getSearchString() {
		return searchString;
	}

	protected SearchAbstract addAliasedTermToSearchString(String string, String alias) {
		addToSearchString(alias + ":" + string);
		return this;
	}

	public static class Term {

		private final double value;
		private final String string;
		private final String alias;
		private final boolean isQuoted;
		private final Mode mode;

		public static final String EMPTY_ALIAS = "";

		private Term(String string, boolean isQuoted, Mode mode, String alias) {
			this.string = string;
			this.mode = mode;
			this.isQuoted = isQuoted;
			this.alias = alias == null ? EMPTY_ALIAS : alias.isEmpty() ? EMPTY_ALIAS : alias;
			this.value = calculateValue();
		}

		public String getString() {
			return string;
		}

		public double getValue() {
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

		static final double CONTAINS_EXACT_MATCH_VALUE = 10000;
		static final double CONTAINS_INSENSITIVE_MATCH_VALUE = 1000;
		static final double CONTAINS_QUOTED_SEARCH_WORD_VALUE = 100;
		static final double CONTAINS_WANTED_SEARCH_WORD_VALUE = 100;
		static final double CONTAINS_SEARCH_WORD_VALUE = 10;
		static final double CONTAINS_UNWANTED_SEARCH_WORD_VALUE = -2;

		private double calculateValue() {
			return (11.0 + (isQuoted() ? 9 : 0))
					* (hasAlias() ? 10 : 1)
					* (isPlus() ? 10 : 1)
					* (isMinus() ? -7 : 1);
		}

		public boolean isMinus() {
			return this.mode.equals(Mode.MINUS);
		}

		public boolean isPlus() {
			return this.mode.equals(Mode.PLUS);
		}

		public boolean isQuoted() {
			return this.isQuoted;
		}

		public boolean aliasMatches(String columnAlias) {
			return getAlias().equalsIgnoreCase(columnAlias);
		}
	}

	public static enum Mode {
		PLUS(),
		MINUS(),
		NORMAL();

		private Mode() {
		}
	}
}
