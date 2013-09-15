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
package nz.co.gregs.dbvolution.h2;

import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.operators.DBEqualsCaseInsensitiveOperator;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;


public class OuterJoinTest extends AbstractTest {
    
    @Test
    public void testANSIJoinClauseCreation(){
        String lineSep = System.getProperty("line.separator");
        Marque mrq = new Marque();
        mrq.setDatabase(database);
        CarCompany carCo = new CarCompany();
        carCo.setDatabase(database);
        System.out.println(""+mrq.getRelationshipsAsSQL(carCo));
        System.out.println(""+carCo.getRelationshipsAsSQL(mrq));
        
        //MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY
        Assert.assertThat(mrq.getRelationshipsAsSQL(carCo).trim(), is("MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY"));
        //CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY
        Assert.assertThat(carCo.getRelationshipsAsSQL(mrq).trim(), is("CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY"));
        
//        mrq.ignoreAllForeignKeys();
        mrq.addRelationship(mrq.name, carCo, carCo.name, new DBEqualsCaseInsensitiveOperator());
        System.out.println(""+mrq.getRelationshipsAsSQL(carCo));
        System.out.println(""+carCo.getRelationshipsAsSQL(mrq));
        
        
        Assert.assertThat(mrq.getRelationshipsAsSQL(carCo).trim(), is("MARQUE.FK_CARCOMPANY = CAR_COMPANY.UID_CARCOMPANY"+lineSep +
" and  lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)"));
        Assert.assertThat(carCo.getRelationshipsAsSQL(mrq).trim(), is("lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)"+lineSep +
" and CAR_COMPANY.UID_CARCOMPANY = MARQUE.FK_CARCOMPANY"));
        
        mrq.ignoreAllForeignKeys();
        System.out.println(""+mrq.getRelationshipsAsSQL(carCo));
        System.out.println(""+carCo.getRelationshipsAsSQL(mrq));
        Assert.assertThat(mrq.getRelationshipsAsSQL(carCo).trim(), is("lower(MARQUE.NAME) =  lower(CAR_COMPANY.NAME)"));
        Assert.assertThat(carCo.getRelationshipsAsSQL(mrq).trim(), is("lower(CAR_COMPANY.NAME) =  lower(MARQUE.NAME)"));
    }
    
}
