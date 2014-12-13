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
package nz.co.gregs.dbvolution.operators;

import java.util.ArrayList;
import java.util.Collection;
import static nz.co.gregs.dbvolution.datatypes.QueryableDatatype.getQueryableDatatypeForObject;
import nz.co.gregs.dbvolution.expressions.DBExpression;

public class DBPermittedValuesOperator extends DBMetaOperator {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public DBPermittedValuesOperator(Object... permitted) {
		ArrayList<DBExpression> expressions = new ArrayList<DBExpression>();
		int objectCount = 0;
		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			for (Object obj : permitted) {
				if (obj == null) {
					this.includeNulls = true;
					expressions.add(null);
					objectCount++;
				} else if (obj instanceof Collection) {
					Collection<Object> myList = (Collection) obj;
					for (Object obj1 : myList) {
						if (obj == null) {
							this.includeNulls = true;
						}
						expressions.add(getQueryableDatatypeForObject(obj1));
						objectCount++;
					}
				} else {
					expressions.add(getQueryableDatatypeForObject(obj));
					objectCount++;
				}
			}
			if (objectCount == 0 || expressions.isEmpty()) {
				operator = new DBIsNullOperator();
			} else if (objectCount == 1) {
				operator = new DBEqualsOperator(expressions.get(0));
			} else {
				operator = new DBInOperator(expressions);
			}
		}
		operator.includeNulls = this.includeNulls;
	}
}
