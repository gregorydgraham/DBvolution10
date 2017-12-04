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

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.columns.ColumnProvider;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author gregorygraham
 */
public class UnableToCreateAscendingExpressionForRecursiveQuery extends DBRuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * For some reason beyond my knowledge the column provided can not be used to
	 * create an ascending recursive query.
	 *
	 * <p>
	 * While this is highly unusual it may be because the datatypes of the FK and
	 * the associated Primary Key disagree: e.g. one is a DBString and the other a
	 * DBInteger.
	 *
	 * @param keyToFollow
	 * @param originatingRow
	 */
	public UnableToCreateAscendingExpressionForRecursiveQuery(ColumnProvider keyToFollow, DBRow originatingRow) {
		super("Unable To Create Ascending Expression For Recursive Query: some combination of the datatypes in " + keyToFollow.getColumn().getPropertyWrapper().javaName() + " and " + originatingRow.getClass().getSimpleName() + " prevents ascending queries working, please check them.");
	}

}
