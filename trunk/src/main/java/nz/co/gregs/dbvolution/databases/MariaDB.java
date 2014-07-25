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

import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.MariaDBDefinition;

/**
 *
 * @author gregory.graham
 */
public class MariaDB extends DBDatabase{

    public final static String MARIADBDRIVERNAME = "com.mariadb.jdbc.Driver";
    protected String databaseName;

    public MariaDB(String jdbcURL, String username, String password) {
        super(new MariaDBDefinition(), MARIADBDRIVERNAME, jdbcURL, username, password);
    }

    public MariaDB(String server, long port, String databaseName, String username, String password) {
        super(new MariaDBDefinition(),
                MARIADBDRIVERNAME,
                "jdbc:mariadb://" + server + ":" + port + "/" + databaseName,
                username,
                password);
        this.setDatabaseName(databaseName);
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
