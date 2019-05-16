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
import nz.co.gregs.dbvolution.expressions.IntegerExpression;
import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 *
 * @author gregorygraham
 */
public class SearchString extends SearchAbstract {

	protected SearchString() {
	}

	public SearchString(String searchTerms) {
		super();
		setSearchString(searchTerms);
	}

//	@Override
//	public Term[] getSearchTerms() throws NothingToSearchFor {
//		List<Term> results = new ArrayList<>(0);
//		SeparatedString separated = SeparatedString.bySpaces();
//		if (getSearchString() != null) {
//			String replaced = getSearchString();
//			replaced = processQuotedTerms(replaced, results, separated);
//			separated.addAll(processUnquotedTerms(replaced, results));
//			results.add(new Term(separated.toString(), CONTAINS_EXACT_MATCH_VALUE));
//			return results.toArray(new Term[]{});
//		} else {
//			throw new NothingToSearchFor();
//		}
//	}

//	public IntegerExpression getRankingExpression(ExpressionAlias col) {
//		final AnyExpression column = col.getExpr();
//		if (column instanceof ExpressionHasStandardStringResult) {
//			StringExpression stringExpression = ((ExpressionHasStandardStringResult) column).stringResult();
//			try {
//				IntegerExpression expr = new IntegerExpression(0);
//				final SearchString.Term[] searchTerms = this.getSearchTerms();
//				for (SearchString.Term term : searchTerms) {
//					IntegerExpression newExpr = getRankingExpressionForTerm(stringExpression, term, col.getAlias());
//					expr = expr.plus(newExpr);
//				}
//				return expr;
//			} catch (SearchString.NothingToSearchFor ex) {
//				return IntegerExpression.value(-1);
//			}
//		}
//		return IntegerExpression.value(-1);
//	}

	public BooleanExpression getComparisonExpression(ExpressionAlias col) {
		return this.getRankingExpression(col).isGreaterThan(0);
	}

	public IntegerExpression getRankingExpression(StringExpression col) {
		StringExpression stringExpression = col;
		try {
			IntegerExpression expr = new IntegerExpression(0);
			final SearchString.Term[] searchTerms = this.getSearchTerms();
			for (SearchString.Term term : searchTerms) {
				IntegerExpression newExpr = getRankingExpressionForTerm(stringExpression, term, term.getAlias());
				expr = expr.plus(newExpr);
			}
			return expr;
		} catch (SearchString.NothingToSearchFor ex) {
			return IntegerExpression.value(-1);
		}
	}

	public BooleanExpression getComparisonExpression(StringExpression col) {
		return this.getRankingExpression(col).isGreaterThan(0);
	}
}
