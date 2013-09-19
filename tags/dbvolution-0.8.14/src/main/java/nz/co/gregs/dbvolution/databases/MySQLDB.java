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

import java.util.Date;
import nz.co.gregs.dbvolution.databases.definitions.MySQLDBDefinition;

/**
 *
 * @author gregory.graham
 */
public class MySQLDB extends DBDatabase{
    
    public final static String MYSQLDRIVERNAME = "com.mysql.jdbc.Driver";
    
    public MySQLDB(String jdbcURL, String username, String password){
        super(new MySQLDBDefinition(), MYSQLDRIVERNAME, jdbcURL, username, password);
    }    
}
