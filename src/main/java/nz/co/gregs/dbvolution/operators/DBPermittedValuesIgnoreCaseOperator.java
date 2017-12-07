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
import nz.co.gregs.dbvolution.expressions.StringExpression;

/**
 * Provides an operator that checks that the column matches the provided values,
 * while remaining case-insensitive.
 *
 * Creates a virtual operator that provides access to the ISNULL, EQUALS, or IN
 * operator as required while ensuring that the actual operation is
 * case-insensitive.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class DBPermittedValuesIgnoreCaseOperator extends DBMetaOperator {

	private static final long serialVersionUID = 1L;

	/**
	 * Provides an operator that checks that the column matches the provided
	 * values, while remaining case-insensitive.
	 *
	 * Creates a virtual operator that provides access to the ISNULL, EQUALS, or
	 * IN operator as required while ensuring that the actual operation is
	 * case-insensitive.
	 *
	 * @param permitted a list of strings that are permitted values.
	 */
	public DBPermittedValuesIgnoreCaseOperator(String... permitted) {

		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			ArrayList<DBExpression> qdts = new ArrayList<DBExpression>();
			for (String obj : permitted) {
				qdts.add(getQueryableDatatypeForObject(obj));
			}
			if (qdts.isEmpty()) {
				operator = new DBIsNullOperator();
			} else if (qdts.size() == 1) {
				operator = new DBEqualsIgnoreCaseOperator(qdts.get(0));
			} else {
				operator = new DBInIgnoreCaseOperator(qdts);
			}
		}
	}

	/**
	 * Provides an operator that checks that the column matches the provided
	 * values, while remaining case-insensitive.
	 *
	 * Creates a virtual operator that provides access to the ISNULL, EQUALS, or
	 * IN operator as required while ensuring that the actual operation is
	 * case-insensitive.
	 *
	 * @param permitted a list of string expression that are permitted values.
	 */
	public DBPermittedValuesIgnoreCaseOperator(StringExpression[] permitted) {

		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			{
				ArrayList<DBExpression> qdts = new ArrayList<DBExpression>();
				for (StringExpression obj : permitted) {
					qdts.add(getQueryableDatatypeForObject(obj));
				}
				if (qdts.isEmpty()) {
					operator = new DBIsNullOperator();
				} else if (qdts.size() == 1) {
					operator = new DBEqualsIgnoreCaseOperator(qdts.get(0));
				} else {
					operator = new DBInIgnoreCaseOperator(qdts);
				}
			}
		}
	}

	/**
	 * Provides an operator that checks that the column matches the provided
	 * values, while remaining case-insensitive.
	 *
	 * Creates a virtual operator that provides access to the ISNULL, EQUALS, or
	 * IN operator as required while ensuring that the actual operation is
	 * case-insensitive.
	 *
	 * @param permitted a list of strings that are permitted values.
	 */
	public DBPermittedValuesIgnoreCaseOperator(Collection<String> permitted) {

		if (permitted == null) {
			operator = new DBIsNullOperator();
		} else {
			ArrayList<DBExpression> qdts = new ArrayList<DBExpression>();
			for (String obj : permitted) {
				qdts.add(getQueryableDatatypeForObject(obj));
			}
			if (qdts.isEmpty()) {
				operator = new DBIsNullOperator();
			} else if (qdts.size() == 1) {
				operator = new DBEqualsIgnoreCaseOperator(qdts.get(0));
			} else {
				operator = new DBInIgnoreCaseOperator(qdts);
			}
		}
	}

}
