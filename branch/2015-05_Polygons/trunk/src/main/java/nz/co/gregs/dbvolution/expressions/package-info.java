/**
 * Make Complex Conditions For Your Queries Using Expressions .
 * <p>
 * DBExpressions like
 * {@link nz.co.gregs.dbvolution.expressions.StringExpression}, {@link nz.co.gregs.dbvolution.expressions.NumberExpression},
 * and {@link nz.co.gregs.dbvolution.expressions.DateExpression} allow the
 * construction of complicated query criteria and conditions.
 * <p>
 * The primary mechanism for creating expressions is chaining, so create an
 * expression from a DBRow field/column or a variable using the standard
 * constructor like {@link nz.co.gregs.dbvolution.expressions.StringExpression#StringExpression(java.lang.String)
 * } and then use the IDE's code completion to extend the chain and your
 * expression.
 * <p>
 * Add the expression to your query using {@link nz.co.gregs.dbvolution.DBQuery#addCondition(nz.co.gregs.dbvolution.expressions.BooleanExpression)
 * }.
 * <p>
 * You can also add expressions to
 * {@link nz.co.gregs.dbvolution.datatypes QueryableDatatypes} within a
 * {@link nz.co.gregs.dbvolution.DBReport} to generate derived values from
 * database columns.
 * <p>
 * I've been writing SQL for 20 years and DBV is easier, I hope you like it.
 * <p>
 * <b>Gregory Graham</b>
 */
package nz.co.gregs.dbvolution.expressions;
