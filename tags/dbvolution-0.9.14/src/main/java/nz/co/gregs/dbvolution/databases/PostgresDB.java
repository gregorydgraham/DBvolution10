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
package nz.co.gregs.dbvolution.databases;

import nz.co.gregs.dbvolution.DBDatabase;
import javax.sql.DataSource;
import nz.co.gregs.dbvolution.databases.definitions.PostgresDBDefinition;


/**
 *
 * @author Gregory Graham
 */
public class PostgresDB extends DBDatabase {
    
    private final String POSTGRES_DRIVER_NAME ="org.postgresql.Driver";

    public PostgresDB(DataSource ds) {
        super(new PostgresDBDefinition(), ds);
    }

    public PostgresDB(String jdbcURL, String username, String password) {
        super(new PostgresDBDefinition(), "org.postgresql.Driver", jdbcURL, username, password);
    }
    
    public PostgresDB(String hostname, String port, String databaseName, String username, String password) {
        this(hostname, port, databaseName, username, password, null);
    }
    
    public PostgresDB(String hostname, String port, String databaseName, String username, String password, String urlExtras) {
        super(new PostgresDBDefinition(), 
                "org.postgresql.Driver", "jdbc:postgresql://"+hostname+":"+port+"/"+databaseName+(urlExtras==null||urlExtras.isEmpty()?"":"?"+urlExtras), 
                username, password);
    }
    
}
