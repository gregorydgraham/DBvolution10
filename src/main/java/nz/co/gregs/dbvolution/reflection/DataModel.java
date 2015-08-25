/*
 * Copyright 2015 gregory.graham.
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
package nz.co.gregs.dbvolution.reflection;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import nz.co.gregs.dbvolution.DBDatabase;
import nz.co.gregs.dbvolution.DBRow;
import org.reflections.Reflections;

/**
 *
 * @author gregory.graham
 */
public class DataModel {
	
	public static Set<Class<? extends DBDatabase>> getDatabases(){
		Reflections reflections = new Reflections("*");
		return reflections.getSubTypesOf(DBDatabase.class);
	}
	
	public static Set<Method> getDatabaseCreationMethods(){
		Reflections reflections = new Reflections("*");
		return reflections.getMethodsReturn(DBDatabase.class);
	}
	
	public static Set<Method> getDatabaseCreationMethodsWithoutParameters(){
		Reflections reflections = new Reflections("*");
		Set<Method> methodsReturn = reflections.getMethodsReturn(DBDatabase.class);
		Set<Method> parameterlessMethods  = new HashSet<Method>();
		for (Method method : methodsReturn) {
			if (method.getParameterTypes().length==0){
				parameterlessMethods.add(method);
			}
		}
		return parameterlessMethods;
	}
	
	public static Set<Class<? extends DBRow>> getTables(){
		Reflections reflections = new Reflections("*");
		return reflections.getSubTypesOf(DBRow.class);
	}
	
}
