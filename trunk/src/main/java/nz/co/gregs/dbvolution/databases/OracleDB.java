/*
 * Copyright 2014 gregory.graham.
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

package nz.co.gregs.dbvolution.databases;

import javax.sql.DataSource;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Super class for connecting the different versions of the Oracle DB.
 * 
 * <p>
 * You should probably use {@link Oracle11DB} or {@link Oracle12DB} instead.
 * 
 * @author gregory.graham
 * @see Oracle11DB
 * @see Oracle12DB
 */
public abstract class OracleDB extends DBDatabase{

	public OracleDB(DBDefinition definition, DataSource ds) {
		super(definition, ds);
	}

	public OracleDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) {
		super(definition, driverName, jdbcURL, username, password);
	}

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); 
	}
	
}
