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

import nz.co.gregs.dbvolution.datatypes.DBInteger;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.*;

/**
 * A DBRow Java class that represents a view on the "marque" table.
 *
 * <p>
 * &#64;DBSelectQuery annotation supplies a select query to be used instead of a
 * table name when creating queries. This allows you to use raw SQL to create
 * new database entities at runtime. it also provides access to advanced SQL
 * features that DBvolution does not yet support.
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
@DBSelectQuery("select uid_marque, isusedfortafros, fk_carcompany from marque")
@DBTableName("marque")
public class MarqueSelectQuery extends DBRow {

	private static final long serialVersionUID = 1L;

	/**
	 * A DBInteger field representing the "uid_marque" column in the database.
	 *
	 * <p>
	 * &#64;DBPrimaryKey both indicates that the field is the primary key of the
	 * table and should be used to connect other related tables to this table.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 * DBInteger is the usual datatype of database primary keys.
	 *
	 * <p>
	 * DBInteger indicates that the field is INTEGER or NUMBER field that
	 * naturally provides Number values in Java. It has an instance as that just
	 * makes everyone's life easier.
	 *
	 */
	@DBColumn("uid_marque")
	@DBPrimaryKey
	public DBInteger uidMarque = new DBInteger();

	/**
	 * A DBString field representing the "isusedfortafros" column in the database.
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
	@DBColumn("isusedfortafros")
	public DBString isUsedForTAFROs = new DBString();

	/**
	 * A DBInteger field representing the "fk_carcompany" column in the database.
	 *
	 * <p>
	 * &#64;DBForeignKey indicates that this field is a reference to the primary
	 * key of the table represented by CarCompany.
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
	@DBForeignKey(CarCompany.class)
	@DBColumn("fk_carcompany")
	public DBInteger carCompany = new DBInteger();
}
