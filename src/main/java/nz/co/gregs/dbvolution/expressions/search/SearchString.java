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

import nz.co.gregs.dbvolution.expressions.BooleanExpression;
import nz.co.gregs.dbvolution.expressions.NumberExpression;
import nz.co.gregs.dbvolution.expressions.SortProvider;
import nz.co.gregs.dbvolution.expressions.StringExpression;

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
public class SearchString extends SearchAbstract implements HasComparisonExpression, HasRankingExpression {

	private ExpressionAlias expr;

	protected SearchString() {
	}

	public SearchString(StringExpression expressionToSearch, String searchTerms) {
		this(new ExpressionAlias(expressionToSearch), searchTerms);
	}

	public SearchString(ExpressionAlias expressionToSearch, String searchTerms) {
		super();
		this.expr = expressionToSearch;
		setSearchString(searchTerms);
	}

	public SearchString(String searchTerms) {
		super();
		setSearchString(searchTerms);
	}

	

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
	 * alias followed by a colon (alias:term). Searching for any empty value can
	 * be done with an alias followed by empty quotes, for example
	 * description:""</p>
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
	 * @return A boolean representation of the expression's relationship to the
	 * search string
	 */
	@Override
	public BooleanExpression getComparisonExpression() {//ExpressionAlias col) {
		return this.getRankingExpression(/*col*/).isGreaterThan(0);
	}

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
	 * alias followed by a colon (alias:term). Searching for any empty value can
	 * be done with an alias followed by empty quotes, for example
	 * description:""</p>
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
	 * @return A numeric representation of the expression's relationship to the
	 * search string
	 */
	@Override
	public NumberExpression getRankingExpression() {//StringExpression expression) {
//		StringExpression stringExpression = expression;
		NumberExpression resultExpr = NumberExpression.value(0.0);
		final SearchString.Term[] searchTerms = this.getSearchTerms();
		for (SearchString.Term term : searchTerms) {
			NumberExpression rankingExpression = getRankingExpression(expr);
//			NumberExpression newExpr = getRankingExpressionForTerm(stringExpression, term, term.getAlias());
			resultExpr = resultExpr.plus(rankingExpression);
		}
		return resultExpr;
	}

//	public BooleanExpression getComparisonExpression(StringExpression col) {
//		return this.getRankingExpression(col).isGreaterThan(0);
//	}
	public SearchString setExpression(StringExpression expression) {
		this.expr = new ExpressionAlias(expression);
		return this;
	}

	@Override
	public SortProvider ascending() {
		return getRankingExpression().ascending();
	}

	@Override
	public SortProvider descending() {
		return getRankingExpression().descending();
	}
}
