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
package nz.co.gregs.dbvolution.example;

import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.*;
import nz.co.gregs.dbvolution.annotations.*;

/**
 * A DBRow Java class that represents the "car_company" table.
 *
 * <p>
 * &#64;DBTableName annotation allows the class to be renamed to fit better
 * within a Java library while preserving the actual database name.
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
@DBTableName("car_company")
public class CarCompany extends DBRow {

	private static final long serialVersionUID = 1L;

	/**
	 * A DBString field representing the "name" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBString indicates that the field is CHAR or VARCHAR field that naturally
	 * provides String values in Java. It has an instance as that just makes
	 * everyone's life easier.
	 *
	 */
	@DBColumn("name")
	public DBString name = new DBString();

	/**
	 * A DBInteger field representing the "uid_carcompany" column in the database.
	 *
	 * <p>
	 * &#64;DBPrimaryKey both indicates that the field is the primary key of the
	 * table and should be used to connect other related tables to this table.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * DBInteger indicates that the field is INTEGER or NUMBER field that
	 * naturally provides Integer values in Java. It has an instance as that just
	 * makes everyone's life easier.
	 *
	 */
	@DBPrimaryKey
	@DBColumn("uid_carcompany")
	public DBInteger uidCarCompany = new DBInteger();

	/**
	 * Required Default Constructor.
	 *
	 * <p>
	 * Lots of reflection is used in DBvolution so a default no-parameter
	 * constructor is required.
	 */
	public CarCompany() {
	}

	/**
	 * Simple convenience constructor.
	 *
	 * @param name name
	 * @param id id
	 */
	public CarCompany(String name, int id) {
		this.name.setValue(name);
		this.uidCarCompany.setValue(id);
	}
}
