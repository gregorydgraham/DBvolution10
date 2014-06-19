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
package nz.co.gregs.dbvolution;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.sql.SQLException;
import nz.co.gregs.dbvolution.exceptions.UnexpectedNumberOfRowsException;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.CompanyLogo;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.Matchers.*;

/**
 *
 * @author gregorygraham
 */
public class ByteArrayTest extends AbstractTest {

    public ByteArrayTest(Object testIterationName, Object db) {
        super(testIterationName, db);
    }

    @Test
    public void createRowWithByteArray() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException {

        CompanyLogo companyLogo = new CompanyLogo();
        companyLogo.logoID.setValue(1);
        companyLogo.carCompany.setValue(1);//Toyota
        companyLogo.imageFilename.setValue("toyota_logo.jpg");
        companyLogo.imageBytes.setFromFileSystem("toyota_share_logo.jpg");
        database.insert(companyLogo);
        
        CarCompany carCompany = new CarCompany();
        carCompany.name.permittedValuesIgnoreCase("FORD");
        CarCompany ford = database.getDBTable(carCompany).getOnlyRowByExample(carCompany);
        
        companyLogo.logoID.setValue(2);
        companyLogo.carCompany.setValue(ford.uidCarCompany.getValue());
        companyLogo.imageFilename.setValue("ford_logo.jpg");
        companyLogo.imageBytes.setFromFileSystem("ford_logo.jpg");
        database.insert(companyLogo);
        
        CompanyLogo logoExample = new CompanyLogo();
        logoExample.carCompany.permittedValues(ford.uidCarCompany);
        List<CompanyLogo> foundLogo = database.get(logoExample);
        database.print(foundLogo);
    }

    @Test
    public void retrieveRowWithByteArray() throws FileNotFoundException, IOException, SQLException, UnexpectedNumberOfRowsException, ClassNotFoundException, InstantiationException {

        CompanyLogo companyLogo = new CompanyLogo();
//        database.print(database.get(companyLogo));
        companyLogo.logoID.setValue(1);
        companyLogo.carCompany.setValue(1);//Toyota
        companyLogo.imageFilename.setValue("toyota_logo.jpg");
        File image = new File("toyota_share_logo.jpg");
        companyLogo.imageBytes.setFromFileSystem(image);
        database.insert(companyLogo);

        File newFile = new File("found_toyota_logo.jpg");
        newFile.delete();
        
        companyLogo = new CompanyLogo();
        CompanyLogo firstRow = database.getDBTable(companyLogo).getRowsByPrimaryKey(1).get(0);
        System.out.println("row = " + firstRow.toString());
        firstRow.imageBytes.writeToFileSystem(newFile.getName());
        newFile = new File(newFile.getName());
        Assert.assertThat(newFile.length(), is(image.length()));
    }
}
