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
package nz.co.gregs.dbvolution.databases;

import nz.co.gregs.dbvolution.DBDatabase;


public class MySQLMXJDB extends MySQLDB {

    public MySQLMXJDB(String jdbcURL, String username, String password) {
        super(jdbcURL, username, password);
    }

    public MySQLMXJDB(String server, long port, String databaseName,String databaseDir, String username, String password) {
        super("jdbc:mysql:mxj://"+server+":" + port + "/" + databaseName
                + "?" + "server.basedir=" + databaseDir
                + "&" + "createDatabaseIfNotExist=true"
                + "&" + "server.initialize-user=true", 
                username, 
                password);
                this.databaseName = databaseName;
    }    

	@Override
	public DBDatabase clone() throws CloneNotSupportedException {
		return super.clone(); //To change body of generated methods, choose Tools | Templates.
	}
}
