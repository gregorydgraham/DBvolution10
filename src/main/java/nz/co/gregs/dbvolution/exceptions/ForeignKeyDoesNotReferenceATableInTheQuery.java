/*
 * Copyright 2015 gregorygraham.
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
package nz.co.gregs.dbvolution.exceptions;

import nz.co.gregs.dbvolution.columns.ColumnProvider;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class ForeignKeyDoesNotReferenceATableInTheQuery extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates an exception indicating that the column provided is not a foreign
	 * key referenced in the query.
	 *
	 * @param keyToFollow the column that is not a foreign key to a table in the query
	 */
	public ForeignKeyDoesNotReferenceATableInTheQuery(ColumnProvider keyToFollow) {
	}

}
