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
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import static nz.co.gregs.dbvolution.datatypes.QueryableDatatype.getQueryableDatatypeForObject;

public class DBPermittedValuesOperator extends DBMetaOperator {

	public static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public DBPermittedValuesOperator(Object... permitted) {
		ArrayList<QueryableDatatype> qdts = new ArrayList<QueryableDatatype>();
		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			for (Object obj : permitted) {
				if (obj == null) {
					this.includeNulls = true;
				} else if (obj instanceof Collection) {
					Collection<Object> myList = (Collection) obj;
					for (Object obj1 : myList) {
						qdts.add(getQueryableDatatypeForObject(obj1));
					}
				} else {
					qdts.add(getQueryableDatatypeForObject(obj));
				}
			}
			if (qdts.isEmpty()) {
				operator = new DBIsNullOperator();
			} else if (qdts.size() == 1) {
				operator = new DBEqualsOperator(qdts.get(0));
			} else {
				operator = new DBInOperator(qdts);
			}
		}
		operator.includeNulls = this.includeNulls;
	}
}
