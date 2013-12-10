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


public class PostgresDBOverSSL extends PostgresDB {

    public PostgresDBOverSSL(String hostname, String port, String databaseName, String username, String password, String urlExtras) {
        super(hostname, port, databaseName, username, password, "ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"+(urlExtras==null||urlExtras.isEmpty()?"":"&"+urlExtras));
    }

    public PostgresDBOverSSL(String hostname, String port, String databaseName, String username, String password) {
        this(hostname, port, databaseName, username, password, "");
    }
    
}
