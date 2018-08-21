/*
 * Copyright 2013 Gregory Graham.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nz.co.gregs.dbvolution.expressions;

import java.util.Set;
import nz.co.gregs.dbvolution.DBReport;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;

/**
 * Interface to be implemented by all DBvolution objects that produce an SQL
 * snippet.
 *
 * <p>
 * An SQL snippet may be a column name, a function, a keyword, or a java value
 * translated to SQL syntax.
 *
 * <p>
 * The actual snippet is produced by the
 * {@link #toSQLString(nz.co.gregs.dbvolution.databases.definitions.DBDefinition) toSQString method}.
 *
 * <p>
 * The {@link #copy() copy() method} allows DBvolution to maintain immutability.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public interface DBExpression {

	/**
	 * Provides a blank instance of the {@link QueryableDatatype} used by this
	 * expression.
	 *
	 * <p>
	 * You are probably looking for {@link ExpressionColumn#asExpressionColumn()
	 * }.
	 *
	 * <p>
	 * Note that this method is not good for use in everyday DBvolution code and
	 * should probably be reserved for meta-programming.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return the QueryableDatatype subclass that corresponds to the results of
	 * this expression
	 */
	public QueryableDatatype<?> getQueryableDatatypeForExpressionValue();

	/**
	 * Produces the snippet provided by this class.
	 *
	 * <p>
	 * This is only used internally.
	 *
	 * <p>
	 * If you are extending DBvolution and adding a new function this is the place
	 * to format the information for use in SQL. A DBDefinition instance is
	 * provided to supply context and so your SQL can used on multiple database
	 * engines.
	 *
	 * @param defn the target database
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the DBValue formatted as a SQL snippet
	 */
	public String toSQLString(DBDefinition defn);

	/**
	 * A Complete Copy Of This DBValue.
	 *
	 * <p>
	 * Immutability in DBvolution is maintain by internally copying objects.
	 *
	 * <p>
	 * This method enables immutability by performing a deep copy of the object.
	 *
	 * <p>
	 * Singletons may return themselves but all other objects must return a new
	 * instance with copies of all mutable fields.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a copy of this {@code DBValue}
	 */
	public DBExpression copy();

	/**
	 * Returns TRUE if this expression is an Aggregator like SUM() or LEAST().
	 *
	 * <p>
	 * Subclasses must implement this method returning TRUE if the expression will
	 * combine the results of several rows to produce a result. If the expression
	 * relies on subexpressions, then the isAggregator method must return TRUE if
	 * the subexpressions include an aggregator.
	 *
	 * <p>
	 * Aggregators collect several rows together to produce a single result.
	 * Examples are MAX, MIN, and AVERAGE.
	 *
	 * <p>
	 * They are only appropriate in the SELECT clause and generally require the
	 * GROUP BY clause to be useful.
	 *
	 * <p>
	 * Aggregators are used with {@link DBReport }. Aggregator expressions are
	 * included in the SELECT clause but excluded from the GROUP BY clause.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if this DBExpression represents an aggregating functions
	 */
	public boolean isAggregator();

	/**
	 * Returns a Set of the DBRow instances involved in this expression.
	 *
	 * <p>
	 * Used by QueryGraph to plot the connections between tables and avoid
	 * cartesian joins.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return a set of DBRow instances involved in this DBExpression.
	 */
	public Set<DBRow> getTablesInvolved();

	/**
	 * Indicates whether or not the expression includes table columns.
	 *
	 * <P>
	 * Purely functional expressions use only in-built functions or literal values
	 * to produce results and do not require data from tables.
	 *
	 * <p>
	 * Some databases, notably MS SQLServer, can not group purely functional
	 * expressions.
	 *
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 *
	 * @return TRUE if the expression does not access table data, otherwise FALSE.
	 */
	public boolean isPurelyFunctional();

	public boolean isComplexExpression();

	public String createSQLForFromClause(DBDatabase database);

	public String createSQLForGroupByClause(DBDatabase database);
}
