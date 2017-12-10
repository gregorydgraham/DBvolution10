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

/**
 * Provides an operator that checks that the column matches the provided values.
 *
 * Creates a virtual operator that provides access to the ISNULL, EQUALS, or IN
 * operator as required.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 * @param <T>
 */
public class DBPermittedValuesOperator<T> extends DBMetaOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Provides an operator that checks that the column matches the provided
	 * values.
	 *
	 * Creates a virtual operator that provides access to the ISNULL, EQUALS, or
	 * IN operator as required.
	 *
	 * @param permitted a list of strings that are permitted values.
	 */
	public DBPermittedValuesOperator(Collection<T> permitted) {
		ArrayList<DBExpression> expressions = new ArrayList<>();
		int objectCount = 0;
		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			for (T obj : permitted) {
				if (obj == null) {
					this.includeNulls = true;
					objectCount++;
				} else if (obj instanceof Collection) {
					@SuppressWarnings("unchecked")
					Collection<Object> myList = (Collection) obj;
					for (Object obj1 : myList) {
						if (obj1 == null) {
							this.includeNulls = true;
						} else {
							expressions.add(getQueryableDatatypeForObject(obj1));
							objectCount++;
						}
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

	/**
	 * Provides an operator that checks that the column matches the provided
	 * values.
	 *
	 * Creates a virtual operator that provides access to the ISNULL, EQUALS, or
	 * IN operator as required.
	 *
	 * @param permitted a list of strings that are permitted values.
	 */
	@SuppressWarnings("unchecked")
	public DBPermittedValuesOperator(T... permitted) {
		ArrayList<DBExpression> expressions = new ArrayList<>();
		int objectCount = 0;
		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			for (T obj : permitted) {
				if (obj == null) {
					this.includeNulls = true;
					objectCount++;
				} else if (obj instanceof Collection) {
					Collection<Object> myList = (Collection) obj;
					for (Object obj1 : myList) {
						if (obj1 == null) {
							this.includeNulls = true;
						} else {
							expressions.add(getQueryableDatatypeForObject(obj1));
							objectCount++;
						}
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
