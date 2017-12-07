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
package nz.co.gregs.dbvolution;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
public class SerializableTest extends AbstractTest {

	String filename = "SerializableTest.obj";

	public SerializableTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void saveToFile() throws SQLException {
		try {

			Marque hummerExample = new Marque();
			hummerExample.getName().permittedValues("PEUGEOT", "HUMMER");
			List<Marque> marqueList = database.getDBTable(hummerExample).getRowsByExample(hummerExample);

			try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
				oos.writeObject(marqueList);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		try {
			try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(filename)))) {
				Object object = ois.readObject();
				if (object instanceof List) {
					List<Object> list = (List<Object>) object;
					for (Object obj : list) {
						if (!(obj instanceof Marque)) {
							throw new RuntimeException("Unable to reload the object correctly");
						}
					}
				}
			}
		} catch (ClassNotFoundException | IOException e) {
			throw new RuntimeException(e);
		}
	}
}
