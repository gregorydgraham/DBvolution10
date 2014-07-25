/*
 * Copyright 2013 greg.
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

import java.sql.Connection;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.H2DBDefinition;

/**
 *
 * @author greg
 */
public class H2DB extends DBDatabase {
	protected final Connection storedConnection;

    public H2DB(String jdbcURL, String username, String password) throws SQLException {
        super(new H2DBDefinition(), "org.h2.Driver", jdbcURL, username, password);
		this.storedConnection = getConnection();
		this.storedConnection.createStatement();
    }

    @Override
    public boolean supportsFullOuterJoinNatively() {
        return false;
    }

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}
    
    
}
