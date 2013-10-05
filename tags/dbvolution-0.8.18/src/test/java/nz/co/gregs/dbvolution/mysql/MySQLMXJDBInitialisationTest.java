package nz.co.gregs.dbvolution.mysql;

import java.io.File;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.MySQLDB;
import nz.co.gregs.dbvolution.databases.MySQLMXJDB;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.Test;

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
/**
 *
 * @author gregorygraham
 */
public class MySQLMXJDBInitialisationTest {
    
    private static int nextPort  = 43215;

    @Test
    public void testInstance() throws SQLException {
        DBDatabase database = getMySQLDBInstance();
        database.setPrintSQLBeforeExecuting(true);
        final Marque marque = new Marque();
        final CarCompany carCompany = new CarCompany();

        database.dropTableNoExceptions(marque);
        database.dropTableNoExceptions(carCompany);
        database.createTable(marque);
        database.createTable(carCompany);
        database.dropTableNoExceptions(marque);
        database.dropTableNoExceptions(carCompany);
    }

    public synchronized static MySQLDB getMySQLDBInstance() {
        //long random = Math.round(Math.random()*59000)+5000; //range between 5000 and 640000 for port numbers
        long random = nextPort;
        nextPort++;

        File ourAppDir = new File(System.getProperty("java.io.tmpdir"));
        long port = random; //Integer.parseInt(System.getProperty("c-mxj_test_port", "3336"));
        File databaseDir = new File(ourAppDir, "test-mxj"+random);
        String databaseName = "dbvolutiontest"+random;
        
//        String url = "jdbc:mysql:mxj://localhost:" + port + "/" + databaseName
//                + "?" + "server.basedir=" + databaseDir
//                + "&" + "createDatabaseIfNotExist=true"
//                + "&" + "server.initialize-user=true";

        MySQLDB database = new MySQLMXJDB("localhost", port, databaseName, databaseDir.toString(), "dbvtest", "testpass");
        return database;
    }
}
