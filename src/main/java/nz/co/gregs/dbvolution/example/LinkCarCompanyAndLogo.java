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
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;

/**
 * A DBRow Java class that represents the "lt_carco_logo" table.
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
@DBTableName("lt_carco_logo")
@SuppressWarnings("serial")
public class LinkCarCompanyAndLogo extends DBRow {

	private static final long serialVersionUID = 1L;

	/**
	 * A DBInteger field representing the "fk_car_company" column in the database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * &#64;DBForeignKey indicates that this field is a reference to the primary
	 * key of the table represented by CarCompany.
	 *
	 * <p>
	 * DBInteger indicates that the field is INTEGER or NUMBER field that
	 * naturally provides Integer values in Java. It has an instance as that just
	 * makes everyone's life easier.
	 *
	 */
	@DBForeignKey(CarCompany.class)
	@DBColumn("fk_car_company")
	public DBInteger fkCarCompany = new DBInteger();

	/**
	 * A DBInteger field representing the "fk_company_logo" column in the
	 * database.
	 *
	 * <p>
	 * &#64;DBColumn both indicates that the field is part of the database table
	 * and protects the actual database column name from any refactoring.
	 *
	 * <p>
	 * &#64;DBForeignKey indicates that this field is a reference to the primary
	 * key of the table represented by CompanyLogo.
	 *
	 * <p>
	 * DBInteger indicates that the field is INTEGER or NUMBER field that
	 * naturally provides Integer values in Java. It has an instance as that just
	 * makes everyone's life easier.
	 *
	 */
	@DBForeignKey(CompanyLogo.class)
	@DBColumn("fk_company_logo")
	public DBInteger fkCompanyLogo = new DBInteger();

}
