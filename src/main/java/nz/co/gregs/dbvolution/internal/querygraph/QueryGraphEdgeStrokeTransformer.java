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

import java.awt.BasicStroke;
import java.awt.Stroke;
import java.util.List;
import java.util.Set;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.expressions.DBExpression;
import org.apache.commons.collections15.Transformer;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregory.graham
 */
public class QueryGraphEdgeStrokeTransformer implements Transformer<DBExpression, Stroke> {

	private final DBQuery query;

	/**
	 *
	 * @param originalQuery
	 */
	public QueryGraphEdgeStrokeTransformer(final DBQuery originalQuery) {
		this.query = originalQuery;
	}

	/**
	 *
	 * @param input
	 * <p style="color: #F90;">Support DBvolution at
	 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
	 * @return the Stroke used to draw an edge
	 */
	@Override
	public Stroke transform(DBExpression input) {
		boolean optionalExpr = false;
		List<DBRow> optionalTables = query.getOptionalTables();
		Set<DBRow> tablesInvolved = input.getTablesInvolved();
		for (DBRow table : tablesInvolved) {
			if (optionalTables.contains(table)) {
				optionalExpr = true;
			}
		}
		if (optionalExpr) {
			return STROKE_FOR_OPTIONAL_EXPRESSION;
		} else {
			return STROKE_FOR_REQUIRED_EXPRESSION;
		}
	}

	/**
	 *
	 */
	public static final BasicStroke STROKE_FOR_OPTIONAL_EXPRESSION = new BasicStroke(1.0f,
			BasicStroke.CAP_BUTT,
			BasicStroke.JOIN_MITER,
			2.0f, new float[]{2.0f}, 0.0f);

	/**
	 *
	 */
	public static final BasicStroke STROKE_FOR_REQUIRED_EXPRESSION = new BasicStroke(1.0f,
			BasicStroke.CAP_BUTT,
			BasicStroke.JOIN_MITER);

}
