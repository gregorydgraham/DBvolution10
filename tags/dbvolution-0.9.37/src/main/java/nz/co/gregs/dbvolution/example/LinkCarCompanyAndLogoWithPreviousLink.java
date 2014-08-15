/*
 * Copyright 2014 gregorygraham.
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
package nz.co.gregs.dbvolution.example;

import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.annotations.DBColumn;
import nz.co.gregs.dbvolution.annotations.DBForeignKey;
import nz.co.gregs.dbvolution.annotations.DBTableName;
import nz.co.gregs.dbvolution.datatypes.DBInteger;

/**
 *
 * @author gregorygraham
 */
@DBTableName("lt_carco_logo_with_prev")
@SuppressWarnings("serial")
public class LinkCarCompanyAndLogoWithPreviousLink extends DBRow{
    
    @DBForeignKey(CarCompany.class)
    @DBColumn("fk_car_company")
    public DBInteger fkCarCompany  = new DBInteger();
    
    @DBForeignKey(CompanyLogo.class)
    @DBColumn("fk_company_logo")
    public DBInteger fkCompanyLogo  = new DBInteger();
    
    @DBForeignKey(CompanyLogo.class)
    @DBColumn("fk_prev_company_logo")
    public DBInteger fkPrevCompanyLogo  = new DBInteger();
}
