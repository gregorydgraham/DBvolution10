/*
 * Copyright 2014 gregory.graham.
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
package nz.co.gregs.dbvolution.internal.querygraph;

import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.expressions.DBExpression;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class QueryGraphEdgeLabelTransformer extends ToStringLabeller<DBExpression> {

	private final DBQuery query;

	/**
	 *
	 * @param originalQuery
	 */
	public QueryGraphEdgeLabelTransformer(final DBQuery originalQuery) {
		this.query = originalQuery;
	}

	@Override
	public String transform(DBExpression v) {
		return v.toSQLString(query.getDatabaseDefinition()).replaceAll("[^ ]*\\.", "");
	}

}
