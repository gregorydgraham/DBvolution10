/*
 * Copyright 2013 gregorygraham.
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
package nz.co.gregs.dbvolution.actions;

import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;

/**
 * Used by {@link DBInsert} to insert BLOB columns.
 *
 * @author gregorygraham
 */
public class DBInsertLargeObjects extends DBUpdateLargeObjects {

	public DBInsertLargeObjects(DBRow row) {
		super(row);
	}

	@Override
	protected List<PropertyWrapper> getInterestingLargeObjects(DBRow row) {
		ArrayList<PropertyWrapper> returnList = new ArrayList<PropertyWrapper>();
		for (QueryableDatatype qdt : row.getLargeObjects()) {
			returnList.add(row.getPropertyWrapperOf(qdt));
		}
		return returnList;
	}

	@Override
	protected DBActionList getActions(){//DBRow row) {
		return new DBActionList(new DBInsertLargeObjects(getRow()));
	}

}
