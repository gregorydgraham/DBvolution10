/*
 * Copyright 2013 gregory.graham.
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

import nz.co.gregs.dbvolution.databases.definitions.OracleDBDefinition;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;


public class OracleDB extends DBDatabase {

    public OracleDB(DBDefinition definition, String driverName, String jdbcURL, String username, String password) {
        super(definition, driverName, jdbcURL, username, password);
    }
    
    public OracleDB(String driverName, String jdbcURL, String username, String password) {
        super(new OracleDBDefinition(), driverName, jdbcURL, username, password);
    }
    
    public OracleDB(String host, int port, String serviceName, String username, String password) {
        super(new OracleDBDefinition(), "oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@//"+host+":"+port+":"+serviceName, username, password);
    }
    
}
