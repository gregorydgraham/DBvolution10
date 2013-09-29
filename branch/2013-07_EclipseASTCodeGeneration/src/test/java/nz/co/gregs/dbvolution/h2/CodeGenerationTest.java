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

import java.io.File;

import nz.co.gregs.dbvolution.example.FKBasedFKRecognisor;
import nz.co.gregs.dbvolution.example.UIDBasedPKRecognisor;
import nz.co.gregs.dbvolution.generation.DBTableClassGenerator;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author gregorygraham
 */
public class CodeGenerationTest extends AbstractTest {
	private static final File OUTPUT_DIR = new File("target/test-output"); 

    @BeforeClass
    public static void setupDirs() {
    	OUTPUT_DIR.mkdirs();
    }
    
    @Test
    public void testGenerateFromSchemaWithRecognisor() throws Exception {
        DBTableClassGenerator classGenerator = new DBTableClassGenerator(OUTPUT_DIR, "nz.co.gregs.dbvolution.generation");
        classGenerator.setPrimaryKeyRecogniser(new UIDBasedPKRecognisor());
        classGenerator.setForeignKeyRecogniser(new FKBasedFKRecognisor());
        
        classGenerator.generateClassesFromJDBCURLToDirectory(database);
    }
}
