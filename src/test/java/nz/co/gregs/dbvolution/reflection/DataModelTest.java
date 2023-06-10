/*
 * Copyright 2015 gregorygraham.
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

import nz.co.gregs.dbvolution.example.ExampleEncodingInterpreter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.DBQuery;
import nz.co.gregs.dbvolution.DBQueryRow;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.H2MemoryDB;
import nz.co.gregs.dbvolution.databases.OracleDB;
import nz.co.gregs.dbvolution.databases.SQLiteDB;
import nz.co.gregs.dbvolution.example.CarCompany;
import nz.co.gregs.dbvolution.example.Marque;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.DBRuntimeException;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 *
 *
 * @author gregorygraham
 */
public class DataModelTest extends AbstractTest {

	public DataModelTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Test
	public void testGetDatabases() {
		Set<Class<? extends DBDatabase>> result = DataModel.getUseableDBDatabaseClasses();
		Map<String, Class<? extends DBDatabase>> conMap = new HashMap<>();
		for (Class<? extends DBDatabase> val : result) {
			conMap.put(val.toString(), val);
		}
		Set<String> constr = conMap.keySet();
		List<String> knownStrings = new ArrayList<>();
		knownStrings.add("class nz.co.gregs.dbvolution.reflection.DataModelTest$DatamodelTestDBClass");
		knownStrings.add("class nz.co.gregs.dbvolution.DBDatabaseClusterTest$1");
		knownStrings.add("class nz.co.gregs.dbvolution.SlowSynchingDatabase");
		for (String knownString : knownStrings) {
			if (!constr.contains(knownString)) {
				System.out.println("KNOWN BUT NOT FOUND: " + knownString);
			}
			assertTrue(constr.contains(knownString));
			conMap.remove(knownString);
		}
		for (String foundString : constr) {
			if (!knownStrings.contains(foundString)) {
				System.out.println("FOUND BUT NOT KNOWN: " + foundString);
			}
			assertTrue(knownStrings.contains(foundString));
			conMap.remove(foundString);
		}
		assertThat(result.size(), is(knownStrings.size()));
	}

	@Test
	public void testGetUsableDBDatabaseConstructors() {
		Set<Constructor<DBDatabase>> result = DataModel.getDBDatabaseConstructors();
		Map<String, Constructor<DBDatabase>> conMap = new HashMap<String, Constructor<DBDatabase>>();
		for (Constructor<DBDatabase> constructor : result) {
			conMap.put(constructor.toString(), constructor);
		}
		Set<String> constr = conMap.keySet();
		List<String> knownStrings = new ArrayList<>();
		knownStrings.add("public nz.co.gregs.dbvolution.reflection.DataModelTest$DatamodelTestDBClass() throws java.sql.SQLException");
		knownStrings.add("nz.co.gregs.dbvolution.DBDatabaseClusterTest$1(nz.co.gregs.dbvolution.DBDatabaseClusterTest,nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder) throws java.sql.SQLException");
		knownStrings.add("private nz.co.gregs.dbvolution.SlowSynchingDatabase(nz.co.gregs.dbvolution.databases.settingsbuilders.H2MemorySettingsBuilder) throws java.sql.SQLException");
		for (String knownString : knownStrings) {
			if (!constr.contains(knownString)) {
				System.out.println("NOT FOUND CONSTRUCTOR: " + knownString + "");
				constr.stream().forEachOrdered((t) -> {
					System.out.println("EXISTING CONSTRUCTOR: " + t);
				});
			}
			assertTrue(constr.contains(knownString));
			conMap.remove(knownString);
		}
		for (String constrString : constr) {
			if (!knownStrings.contains(constrString)) {
				System.out.println("UNKNOWN CONSTRUCTOR: " + constrString + "");
				constr.stream().forEachOrdered((t) -> {
					System.out.println("EXPECTED CONSTRUCTOR: " + t);
				});
			}
			assertTrue(constr.contains(constrString));
			conMap.remove(constrString);
		}
		assertThat(result.size(), is(knownStrings.size()));
	}

	public static class DatamodelTestDBClass extends H2MemoryDB {

		private static final long serialVersionUID = 1L;

		public DatamodelTestDBClass() throws SQLException {
		}

		public static DatamodelTestDBClass creation1() throws SQLException {
			return new DatamodelTestDBClass();
		}

		public static DatamodelTestDBClass creation2() throws SQLException {
			return new DatamodelTestDBClass();
		}

	}

	@Test
	public void testGetDBDatabaseConstructorsPublicWithoutParameters() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Set<Constructor<DBDatabase>> result = DataModel.getDBDatabaseConstructorsPublicWithoutParameters();
		for (Constructor<DBDatabase> constr : result) {
			try {
				constr.setAccessible(true);
				DBDatabase newInstance = constr.newInstance();
				assertThat(newInstance, instanceOf(DBDatabase.class));
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				Logger.getLogger(DataModelTest.class.getName()).log(Level.SEVERE, null, ex);
				throw ex;
			}
		}
		assertThat(result.size(), is(1));
	}

	@Test
	public void testGetDBRowClasses() {
		Set<Class<? extends DBRow>> result = DataModel.getDBRowSubclasses();

		Map<String, Class<? extends DBRow>> foundMap = new HashMap<String, Class<? extends DBRow>>();
		for (Class<? extends DBRow> clazz : result) {
			final String clazzName = clazz.toString();
			if (clazzName.startsWith("class nz.co.gregs.dbvolution.generation.compiling.")) {
				// The compilation tests cause classes to pop into and out of existence so ignore them
			} else {
				foundMap.put(clazzName, clazz);
			}
		}
		Set<String> foundKeys = foundMap.keySet();

		Set<String> knownKeys = new HashSet<String>();
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.LineSegment2DExpressionTest$BoundingBoxTest");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.IntegerExpressionTest$CarCompanyWithChoose");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.DateExpressionTest$MarqueWithDateAggregators");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinitionTest$1MyClass2");
		knownKeys.add("class nz.co.gregs.dbvolution.DBQueryInsertTest$Professional");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$27MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithLocalDate");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLocalDateTimeTest$DBLocalDateTimeTable");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBEnumTest$StringEnumTable");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateOnlyTest$DateOnlyTest");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRecursiveQueryTest$PartsStringKey");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$3TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.DateExpressionTest$MarqueWithComplexWindowingFunction");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$11TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.BooleanArrayExpressionTest$BooleanArrayExpressionTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$7TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.DateExpressionTest$MarqueWithSecondsFromDate");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.NumberExpressionTest$degreeRow");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBEnumTest$IntegerEnumWithDefinedValuesTable");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.Marque");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.DBRowClassWrapperUsabilityTest$MyExampleTableClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$10MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DBScriptTest$ScriptTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRecursiveQueryTest$PartsWithoutTableName$ParentPart");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBBooleanArrayTest$BooleanArrayTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithComplexWindowingFunction");
		knownKeys.add("class nz.co.gregs.dbvolution.DBMigrationTest$Villain");
		knownKeys.add("class nz.co.gregs.dbvolution.DBQueryInsertTest$Fight");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$8TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.DoubleJoinTest$DoubleLinkedWithSubclasses");
		knownKeys.add("class nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest$TableAString");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.StringExpressionTest$MarqueWithLagAndLeadFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$1TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBNumberTest$NumberTest");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLargeTextTest$CompanyTextForRetreivingBinaryObject");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBDeleteTest$TestDeleteAll");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithEndOfMonthForInstantColumn");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$7MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$31MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$12TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest$TableB");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithLocalDateTime");
		knownKeys.add("class nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$8MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$6TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.Line2DExpressionTest$BoundingBoxTest");
		knownKeys.add("class nz.co.gregs.dbvolution.OuterJoinTest$Antagonist");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpressionTest$PolygonIntersectionTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseClusterTest$DBDatabaseClusterTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DoubleJoinTest$Manufacturer");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.BooleanExpressionTest$MarqueWithLeadAndLagFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorTest$CustomerWithDBStringIntegerTypeAdaptor");
		knownKeys.add("class nz.co.gregs.dbvolution.JoinTest$CompanyWithFkToPk");
		knownKeys.add("class nz.co.gregs.dbvolution.OuterJoinTest$Antagonist$NPC");
		knownKeys.add("class nz.co.gregs.dbvolution.JoinTest$Statistic");
		knownKeys.add("class nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest$TableC");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$4TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.DBMigrationTest$MigrateVillainToProfessional");
		knownKeys.add("class nz.co.gregs.dbvolution.annotations.AutoFillDuringQueryIfPossibleTest$FilledMarque");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$1TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.BooleanExpressionTest$MarqueWithBooleanExpressionCount");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$35MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLargeBinaryTest$BinaryObjectWithAutoIncrement");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithLagAndLeadFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$16MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.DBQueryInsertTest$MigrateHeroAndVillianToFight");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRecursiveQueryTest$PartsWithoutTableName");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.StringExpressionTest$trimmedMarque");
		knownKeys.add("class nz.co.gregs.dbvolution.OuterJoinTest$Antagonist$Dragon");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRecursiveQueryTest$Parts$ParentPart");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.search.SearchStringTest$SearchMarque");
		knownKeys.add("class nz.co.gregs.dbvolution.DBMigrationTest$MigrateHeroAndVillianToFight");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$36MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$TestDefaultValueIncorrectDatatype");
		knownKeys.add("class nz.co.gregs.dbvolution.annotations.AutoFillDuringQueryIfPossibleTest$FilledCarCoWithArray");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$2TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.NumberExpressionTest$ExtendedCarCompany");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$1MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBBooleanTest$BooleanTest");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.DateExpressionTest$MarqueWithEndOfMonthForDateColumn");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$9TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$9MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.IntegerExpressionTest$degreeRow");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.MultiPoint2DExpressionTest$MultiPoint2DTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest$TableE");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithAggregatorAndDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$Address");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpressionTest$PolygonTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$3MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$10MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRowMiscTests$SpecifiedColumnName");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$14MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$3MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$15MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBJavaObjectTest$DBJavaObjectTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$2MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$13MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$20MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$5MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$5TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.GeneratedMarqueTest$TestAutoIncrementDetection");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$34MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.NumberExpressionTest$RandomRow");
		knownKeys.add("class nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest$TableA");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$5TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$3TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$22MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$29MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$9MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$2MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRowMiscTests$UnspecifiedColumnName");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithEndOfMonthOfLocalDateTimeColumn");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRecursiveQueryTest$Parts");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.MultiPoint2DExpressionTest$BoundingBoxTest");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$6TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBStatisticsTest$StatsIntegerTest");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$23MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBInstantTest$DBInstantTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinitionTest$2MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.example.Marque");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2DTest$BasicSpatialTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$4TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$37MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.NumberExpressionTest$MarqueWithLagAndLeadFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TableHandlerTest$NonAnnotatedSubclassOfNonAnnotatedDBRow");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.DBRowClassWrapperTest$1TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$DropTableTestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest$TableE");
		knownKeys.add("class nz.co.gregs.dbvolution.OuterJoinTest$Encounter");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.StringExpressionTest$FindFirstIntegerTable");
		knownKeys.add("class nz.co.gregs.dbvolution.MultiplePrimaryKeyTests$RequestingUser");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.BooleanExpressionTest$MarqueWithWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.Companylogo");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBEnumTest$GenericEnumTable");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.GeneratedMarqueTest$CreateTableForeignKeyy");
		knownKeys.add("class nz.co.gregs.dbvolution.example.CompanyText");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$CreateTableTestClassWithNewColumns");
		knownKeys.add("class nz.co.gregs.dbvolution.annotations.AutoFillDuringQueryIfPossibleTest$FilledCarCoWithList");
		knownKeys.add("class nz.co.gregs.dbvolution.DBMigrationTest$Fight");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$TestValueRetrievalWith2PKs");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorTest$CustomerWithStringIntegerTypeAdaptor");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$4TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperTest$2MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$24MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$21MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.DBValidationTest$Villain");
		knownKeys.add("class nz.co.gregs.dbvolution.DBValidationTest$MigrateJamesAndAllVillainsToFight");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseClusterTest$DBDatabaseClusterTestTable2");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.IntegerExpressionTest$CarCompanyWithChooseWithDefault");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$CreateTableTestClass2");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithSecondsFromDate");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithSecondsFromDate");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLargeBinaryTest$CompanyLogoForRetreivingString");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLargeBinaryTest$CompanyLogoForRetreivingBinaryObject");
		knownKeys.add("class nz.co.gregs.dbvolution.OuterJoinTest$Antagonist$Monster");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithDateAggregators");
		knownKeys.add("class nz.co.gregs.dbvolution.MultiplePrimaryKeyTests$InvitedUser");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperTest$1MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$33MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.Point2DExpressionTest$PointTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithComplexWindowingFunction");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$5MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$MarqueWithDateRepeatExprCol");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$DateRepeatYears");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLargeTextTest$CompanyTextForRetreivingString");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRecursiveQueryTest$CompletePart");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$5TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.DBRowClassWrapperTest$MyTable1");
		knownKeys.add("class nz.co.gregs.dbvolution.example.CompanyLogo");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithComplexWindowingFunction");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TableHandlerTest$MyNonAnnotatedDBRow");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$4MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBBulkInsertTest$BulkInsertTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$26MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.Point2DExpressionTest$BoundingBoxTest");
		knownKeys.add("class nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest$TableB");
		knownKeys.add("class nz.co.gregs.dbvolution.DBQueryHavingTest$MarqueCounter");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBStatisticsTest$MedianExpressionTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.NumberExpressionTest$CountIfRow");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$AddressWithCircularReferenceToCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.DBQueryInsertTest$Villain");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$6TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.BooleanExpressionTest$MarqueWithIfThenElse");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRowMiscTests$WithoutPrimaryKey");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.NumberExpressionTest$CarCompanyWithChooseWithDefault");
		knownKeys.add("class nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest$TableD");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBStatisticsTest$StatsOfUpdateCountTest");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperTest$2MyClass1");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$13TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.DBValidationTest$Hero");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinitionTest$1MyClass1");
		knownKeys.add("class nz.co.gregs.dbvolution.DBQueryInsertTest$Hero");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.NumberExpressionTest$CarCompanyWithChoose");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithDateAggregators");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperTest$1MyClass2");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseClusterTest$TableThatDoesExistOnTheCluster");
		knownKeys.add("class nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest$TableA");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$25MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.Point2DExpressionTest$DistanceTest");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$DropTable2TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.LtCarcoLogo");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$CustomerWithCircularReferenceToAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorTest$CustomerWithDBInteger");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseClusterTest$TableThatDoesntExistOnTheCluster");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithDateAggregators");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$DateRepeatMonths");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$DateRepeatTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$17MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithEndOfMonthForInstantColumn");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$7MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$CreateTableTestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.MultiplePrimaryKeyTests$User");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$DateRepeatSeconds");
		knownKeys.add("class nz.co.gregs.dbvolution.JoinTest$CompanyWithFkToNonPk");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$CreateTableWithForeignKeyTestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$38MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.DBMigrationTest$Hero");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithAggregatorAndDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$12MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$CreateTableWithForeignKeyTestClass2");
		knownKeys.add("class nz.co.gregs.dbvolution.DBMigrationTest$Professional");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithAggregatorAndDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$TestInsertDoesNotUpdateExpressionColumns");
		knownKeys.add("class nz.co.gregs.dbvolution.ExpressionsInDBRowFields$ExpressionRow");
		knownKeys.add("class nz.co.gregs.dbvolution.example.MarqueSelectQuery");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$11MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBStatisticsTest$StatsStringTest");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperDefinitionTest$1MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.IntegerExpressionTest$CountIfRow");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.DateExpressionTest$MarqueWithDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$DateRepeatHours");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$RequiredTableShouldBeCreatedAutomatically");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBDeleteTest$TestDeleteThrowsExceptionOnBlankRow");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$Customer");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$DateRepeatDays");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.Spatialgen");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.Spatialgen");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLargeTextTest$TextObjectWithAutoIncrement");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$9TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$2TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBActionListCreationTest$CarCompanyWithAutoIncrement");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseTest$CreateTableTestClassWithOriginalColumns");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBPasswordHashTest$PasswordTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$2TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.MultiplePrimaryKeyTests$Colleagues");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.Line2DExpressionTest$LineTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.search.SearchAcrossTest$SearchAcrossMarque");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$4MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$6MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.IntegerExpressionTest$MarqueWithLagAndLeadFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.DBValidationTest$Fight");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TableHandlerTest$NonAnnotatedSubclassOfAnnotatedDBRow");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$28MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$10TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.DoubleJoinTest$DoubleLinkedWithClass");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest$DateRepeatMinutes");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$TestDefaultInsertValue");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$1MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TypeAdaptorUsabilityTest$6MyTable");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperTest$1MyClass1");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$30MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$8TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.StringExpressionTest$FindFirstNumberTable");
		knownKeys.add("class nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogoWithPreviousLink");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$19MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithEndOfMonthForLocalDateColumn");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TableHandlerTest$MyAnnotatedDBRow");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$8MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest$MarqueWithLagAndLeadFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$7TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyWrapperTest$3MyClass1");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.CarCompany");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBLocalDateTest$DBLocalDateTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DBQueryInsertTest$MigrateVillainToProfessional");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.TableHandlerTest$AnnotatedSubclassOfAnnotatedDBRow");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBEnumTest$IntegerEnumTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.InstantExpressionTest$MarqueWithInstant");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.DateExpressionTest$MarqueWithAggregatorAndDateWindowingFunctions");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.EnumTypeHandlerTest$7TestClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$1TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$18MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.DBRowClassWrapperTest$MyTable2");
		knownKeys.add("class nz.co.gregs.dbvolution.example.CarCompany");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$3TestAddress");
		knownKeys.add("class nz.co.gregs.dbvolution.DoubleJoinTest$Marketer");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.spatial2D.LineSegment2DExpressionTest$LineSegmentTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DBRecursiveQueryTest$PartsStringKey$ParentPart");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$TestDefaultValueRetrieval");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.PropertyTypeHandlerTest$32MyClass");
		knownKeys.add("class nz.co.gregs.dbvolution.internal.properties.ForeignKeyHandlerTest$8TestCustomer");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.IntegerExpressionTest$ExtendedCarCompany");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.BooleanExpressionTest$MarqueWithEquivalentCaseStatements");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$TestDefaultInsertWithLocalDateTimeValue");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$TestDefaultInsertWithInstantValue");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest$MarqueWithEndOfMonthForLocalDateTimeColumn");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBDurationTest$DurationTable");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBEncryptedTextTest$EncryptedTextTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBUUIDTest$UUIDTestTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.JavaObjectExpressionTest$JavaObjectExpressionTable");
		knownKeys.add("class nz.co.gregs.dbvolution.expressions.LargeTextExpressionTest$ExampleTableForLargeText");
		knownKeys.add("class nz.co.gregs.dbvolution.datatypes.DBNumberStatisticsTest$DBNumberStatisticsTestDBRowClass");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.DataRepoGeneratorTest$CreateTableForeignKey");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.DataRepoGeneratorTest$CreateTableForeignKeyy");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.DataRepoGeneratorTest$TestAutoIncrementDetection");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.LtCarcoLogo");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.GeneratedMarqueTest$CreateTableForeignKey");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.Marque");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.Companylogo");
		knownKeys.add("class nz.co.gregs.dbvolution.generation.deprecated.CarCompany");
		knownKeys.add("class nz.co.gregs.dbvolution.OracleCompatibilityTest$CountIfRow");
		knownKeys.add("class nz.co.gregs.dbvolution.DBDatabaseClusterTest$DBDatabaseClusterTestTrackedTable");
		knownKeys.add("class nz.co.gregs.dbvolution.DBMigrationTest$MigrateHeroesAndSelectedVilliansToFight");
		knownKeys.add("class nz.co.gregs.dbvolution.actions.DBInsertTest$testDefaultValuesAreConsistentInCluster");
		knownKeys.add("class nz.co.gregs.dbvolution.databases.metadata.MySQLDBDatabaseMetaData$STGeometryColumns");

		// Store the number found for use later
		final int numberOfClassesFound = foundKeys.size();

		boolean allClassesKnown = true;
		for (String knownString : knownKeys) {
			if (!foundKeys.contains(knownString)) {
				allClassesKnown = false;
				// Remove the classes we know of to help identify the unknown classes
				System.out.println("NOT FOUND DBRow: ~" + knownString + "~");
			}
			foundKeys.remove(knownString);
		}
		// Print out the remain classes as unknown to help with debugging
		for (String foundString : foundKeys) {
			// Double check that we don't know of this class
			if (!knownKeys.contains(foundString)) {
				allClassesKnown = false;
				System.out.println("UNKNOWN DBRow: " + foundString + "");
			}
		}
		// Check that all classes are known and found
		assertThat(allClassesKnown, is(true));
		assertThat(numberOfClassesFound, is(knownKeys.size()));
	}

	@Test
	public void testGetDBRowDirectSubclasses() {
		Set<Class<? extends DBRow>> result = DataModel.getDBRowDirectSubclasses();

		List<String> foundClasses = new ArrayList<String>();
		for (Class<? extends DBRow> clazz : result) {
			final String canonicalName = clazz.getCanonicalName();
			if (canonicalName.startsWith("nz.co.gregs.dbvolution.generation.compiling.")) {
				// The compilation tests cause classes to pop into and out of existence so ignore them
			} else {
				foundClasses.add(canonicalName);
			}
		}

		Set<String> knownKeys = new HashSet<String>();
		knownKeys.add("nz.co.gregs.dbvolution.example.CarCompany");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.TestDefaultValueIncorrectDatatype");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBDeleteTest.TestDeleteAll");
		knownKeys.add("nz.co.gregs.dbvolution.DBMigrationTest.Fight");
		knownKeys.add("nz.co.gregs.dbvolution.DBRowMiscTests.WithoutPrimaryKey");
		knownKeys.add("nz.co.gregs.dbvolution.DBScriptTest.ScriptTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.CreateTableTestClassWithNewColumns");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLargeBinaryTest.BinaryObjectWithAutoIncrement");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.TableHandlerTest.MyNonAnnotatedDBRow");
		knownKeys.add("nz.co.gregs.dbvolution.DBValidationTest.Villain");
		knownKeys.add("nz.co.gregs.dbvolution.generation.CarCompany");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.LocalDateExpressionTest.MarqueWithLocalDate");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.TestDefaultInsertValue");
		knownKeys.add("nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest.TableE");
		knownKeys.add("nz.co.gregs.dbvolution.example.CompanyLogo");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.CreateTableTestClassWithOriginalColumns");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.LocalDateTimeExpressionTest.MarqueWithLocalDateTime");
		knownKeys.add("nz.co.gregs.dbvolution.OuterJoinTest.Encounter");
		knownKeys.add("nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest.TableD");
		knownKeys.add("nz.co.gregs.dbvolution.ExpressionsInDBRowFields.ExpressionRow");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.TestDefaultInsertWithLocalDateTimeValue");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.spatial2D.Point2DExpressionTest.PointTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBNumberTest.NumberTest");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBBooleanTest.BooleanTest");
		knownKeys.add("nz.co.gregs.dbvolution.JoinTest.CompanyWithFkToNonPk");
		knownKeys.add("nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest.TableA");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.JavaObjectExpressionTest.JavaObjectExpressionTable");
		knownKeys.add("nz.co.gregs.dbvolution.generation.DataRepoGeneratorTest.CreateTableForeignKey");
		knownKeys.add("nz.co.gregs.dbvolution.DBQueryInsertTest.Hero");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBInstantTest.DBInstantTable");
		knownKeys.add("nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest.TableE");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.CreateTableWithForeignKeyTestClass2");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.TypeAdaptorTest.CustomerWithStringIntegerTypeAdaptor");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBEnumTest.IntegerEnumWithDefinedValuesTable");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.TypeAdaptorTest.CustomerWithDBStringIntegerTypeAdaptor");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.LtCarcoLogo");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBEnumTest.StringEnumTable");
		knownKeys.add("nz.co.gregs.dbvolution.example.MarqueSelectQuery");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.Marque");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseClusterTest.TableThatDoesntExistOnTheCluster");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.Spatialgen");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBBooleanArrayTest.BooleanArrayTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBDurationTest.DurationTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLargeTextTest.CompanyTextForRetreivingString");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLargeBinaryTest.CompanyLogoForRetreivingString");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.StringExpressionTest.FindFirstNumberTable");
		knownKeys.add("nz.co.gregs.dbvolution.DBValidationTest.Hero");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseClusterTest.TableThatDoesExistOnTheCluster");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.spatial2D.Line2DExpressionTest.LineTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBJavaObjectTest.DBJavaObjectTable");
		knownKeys.add("nz.co.gregs.dbvolution.JoinTest.CompanyWithFkToPk");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.CreateTableWithForeignKeyTestClass");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.DBRowClassWrapperTest.MyTable1");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBEnumTest.GenericEnumTable");
		knownKeys.add("nz.co.gregs.dbvolution.DBRecursiveQueryTest.Parts");
		knownKeys.add("nz.co.gregs.dbvolution.DBQueryInsertTest.Professional");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseClusterTest.DBDatabaseClusterTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.DBRowMiscTests.SpecifiedColumnName");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBEncryptedTextTest.EncryptedTextTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.spatial2D.LineSegment2DExpressionTest.LineSegmentTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBPasswordHashTest.PasswordTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBDateRepeatTest.DateRepeatTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBDateOnlyTest.DateOnlyTest");
		knownKeys.add("nz.co.gregs.dbvolution.generation.Marque");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.StringExpressionTest.FindFirstIntegerTable");
		knownKeys.add("nz.co.gregs.dbvolution.generation.LtCarcoLogo");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.GeneratedMarqueTest.TestAutoIncrementDetection");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.InstantExpressionTest.MarqueWithInstant");
		knownKeys.add("nz.co.gregs.dbvolution.DoubleJoinTest.DoubleLinkedWithClass");
		knownKeys.add("nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest.TableAString");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.DBRowClassWrapperTest.MyTable2");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBBulkInsertTest.BulkInsertTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.DBMigrationTest.Hero");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLocalDateTest.DBLocalDateTable");
		knownKeys.add("nz.co.gregs.dbvolution.DBRecursiveQueryTest.PartsWithoutTableName");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.CreateTableTestClass2");
		knownKeys.add("nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogoWithPreviousLink");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.TableHandlerTest.MyAnnotatedDBRow");
		knownKeys.add("nz.co.gregs.dbvolution.DBMigrationTest.Villain");
		knownKeys.add("nz.co.gregs.dbvolution.DBRecursiveQueryTest.PartsStringKey");
		knownKeys.add("nz.co.gregs.dbvolution.MultiplePrimaryKeyTests.User");
		knownKeys.add("nz.co.gregs.dbvolution.DBMigrationTest.Professional");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.TestDefaultValueRetrieval");
		knownKeys.add("nz.co.gregs.dbvolution.example.Marque");
		knownKeys.add("nz.co.gregs.dbvolution.exceptions.ForeignKeyCannotBeComparedToPrimaryKeyTest.TableB");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.TestInsertDoesNotUpdateExpressionColumns");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLargeTextTest.CompanyTextForRetreivingBinaryObject");
		knownKeys.add("nz.co.gregs.dbvolution.example.CompanyText");
		knownKeys.add("nz.co.gregs.dbvolution.JoinTest.Statistic");
		knownKeys.add("nz.co.gregs.dbvolution.generation.DataRepoGeneratorTest.TestAutoIncrementDetection");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.LargeTextExpressionTest.ExampleTableForLargeText");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.DropTableTestClass");
		knownKeys.add("nz.co.gregs.dbvolution.generation.Companylogo");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBDeleteTest.TestDeleteThrowsExceptionOnBlankRow");
		knownKeys.add("nz.co.gregs.dbvolution.example.LinkCarCompanyAndLogo");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBUUIDTest.UUIDTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBEnumTest.IntegerEnumTable");
		knownKeys.add("nz.co.gregs.dbvolution.generation.Spatialgen");
		knownKeys.add("nz.co.gregs.dbvolution.MultiplePrimaryKeyTests.Colleagues");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBActionListCreationTest.CarCompanyWithAutoIncrement");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.spatial2D.DBPolygon2DTest.BasicSpatialTable");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.GeneratedMarqueTest.CreateTableForeignKeyy");
		knownKeys.add("nz.co.gregs.dbvolution.DBQueryInsertTest.Fight");
		knownKeys.add("nz.co.gregs.dbvolution.DBRowMiscTests.UnspecifiedColumnName");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.TypeAdaptorTest.CustomerWithDBInteger");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.spatial2D.MultiPoint2DExpressionTest.MultiPoint2DTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLargeTextTest.TextObjectWithAutoIncrement");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.CreateTableTestClass");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.GeneratedMarqueTest.CreateTableForeignKey");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseClusterTest.DBDatabaseClusterTestTable2");
		knownKeys.add("nz.co.gregs.dbvolution.DBQueryInsertTest.Villain");
		knownKeys.add("nz.co.gregs.dbvolution.OuterJoinTest.Antagonist");
		knownKeys.add("nz.co.gregs.dbvolution.internal.properties.DBRowClassWrapperUsabilityTest.MyExampleTableClass");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.DropTable2TestClass");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.spatial2D.Polygon2DExpressionTest.PolygonTestTable");
		knownKeys.add("nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest.TableC");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.TestDefaultInsertWithInstantValue");
		knownKeys.add("nz.co.gregs.dbvolution.DBRecursiveQueryTest.CompletePart");
		knownKeys.add("nz.co.gregs.dbvolution.expressions.BooleanArrayExpressionTest.BooleanArrayExpressionTable");
		knownKeys.add("nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest.TableB");
		knownKeys.add("nz.co.gregs.dbvolution.generation.DataRepoGeneratorTest.CreateTableForeignKeyy");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.TestValueRetrievalWith2PKs");
		knownKeys.add("nz.co.gregs.dbvolution.DBValidationTest.Fight");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLargeBinaryTest.CompanyLogoForRetreivingBinaryObject");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.CarCompany");
		knownKeys.add("nz.co.gregs.dbvolution.datatypes.DBLocalDateTimeTest.DBLocalDateTimeTable");
		knownKeys.add("nz.co.gregs.dbvolution.generation.deprecated.Companylogo");
		knownKeys.add("nz.co.gregs.dbvolution.DoubleJoinTest.DoubleLinkedWithSubclasses");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseClusterTest.DBDatabaseClusterTestTrackedTable");
		knownKeys.add("nz.co.gregs.dbvolution.query.QueryGraphDepthFirstTest.TableA");
		knownKeys.add("nz.co.gregs.dbvolution.DBDatabaseTest.RequiredTableShouldBeCreatedAutomatically");
		knownKeys.add("nz.co.gregs.dbvolution.actions.DBInsertTest.testDefaultValuesAreConsistentInCluster");
		knownKeys.add("nz.co.gregs.dbvolution.databases.metadata.MySQLDBDatabaseMetaData.STGeometryColumns");

		// Store the number found for use later
		final int numberOfFoundClasses = foundClasses.size();
		boolean allClassesKnown = true;
		for (String knownString : knownKeys) {
			if (!foundClasses.contains(knownString)) {
				allClassesKnown = false;
				System.out.println("NOT FOUND DBRow: " + knownString);
			}
			// Remove the classes we know of to help identify the unknown classes
			foundClasses.remove(knownString);
		}
		// Print out the remaining classes to help with debugging
		for (String foundString : foundClasses) {
			// Double check that we don't know of this class
			if (!knownKeys.contains(foundString)) {
				allClassesKnown = false;
				System.out.println("UNKNOWN DBRow: " + foundString);
			}
		}
		// Check that all classes are known and found
		assertThat(allClassesKnown, is(true));
		assertThat(numberOfFoundClasses, is(knownKeys.size()));
	}

	@Test
	public void testGetDBDatabaseCreationMethodsStaticWithoutParameters() {
		List<Method> dbDatabaseCreationMethods = DataModel.getDBDatabaseCreationMethodsStaticWithoutParameters();
		for (Method creator : dbDatabaseCreationMethods) {
			creator.setAccessible(true);
			if (database instanceof SQLiteDB) {
				try {
					DBDatabase db = (DBDatabase) creator.invoke(null);
				} catch (Exception ex) {
					System.out.println("EXCEPTION: " + ex.getClass().getCanonicalName());
					System.out.println("EXCEPTION: " + ex.getMessage());
					System.out.println("EXCEPTION: " + ex.getLocalizedMessage());
					fail("Unable to invoke " + creator.getDeclaringClass().getCanonicalName() + "." + creator.getName() + "()");
				}
			}
		}
		assertThat(dbDatabaseCreationMethods.size(), is(3));
	}

	@Test
	public void testCreateDBQueryFromEncodedTablePropertiesAndValues() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-name=TOYOTA&nz.co.gregs.dbvolution.example.Marque",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();
		assertThat(allRows.size(), is(2));
		assertThat(allRows.get(0).get(new CarCompany()).name.stringValue(), is("TOYOTA"));
		assertThat(allRows.get(0).get(new Marque()).name.stringValue(), isOneOf("TOYOTA", "HYUNDAI"));
		assertThat(allRows.get(1).get(new Marque()).name.stringValue(), isOneOf("TOYOTA", "HYUNDAI"));

	}

	@Test
	public void testCreateDBQueryFromEncodedThrowsBlankException() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque",
				new ExampleEncodingInterpreter()
		);
		try {
			List<DBQueryRow> allRows = query.getAllRows();
			throw new DBRuntimeException("Failed To Create AccidentalBlankQueryException!");
		} catch (AccidentalBlankQueryException blank) {
		}
		query.setBlankQueryAllowed(true);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsSimpleIntegerValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=1&nz.co.gregs.dbvolution.example.Marque",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(2));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsDoubleEndedIntegerRange() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=3...4&nz.co.gregs.dbvolution.example.Marque",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(19));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsDownwardOpenEndedIntegerRange() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=...4&nz.co.gregs.dbvolution.example.Marque",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));

	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsUpwardOpeEnded() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=3...&nz.co.gregs.dbvolution.example.Marque",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(19));

	}

	@Test
	@SuppressWarnings("deprecation")
	public void testCreateDBQueryFromEncodedAcceptsSimpleDateValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-creationDate=23 Mar 2013 12:34:56",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
		assertThat(allRows.get(0).get(new Marque()).creationDate.dateValue(), is(new Date("23 Mar 2013 12:34:56")));
	}

	@Test
	@SuppressWarnings("deprecation")
	public void testCreateDBQueryFromEncodedAcceptsDateRangeValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-creationDate=22 Mar 2013 12:34:56...24 Mar 2013 12:34:56",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(18));
		assertThat(allRows.get(0).get(new Marque()).creationDate.dateValue(), is(new Date("23 Mar 2013 12:34:56")));
	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsSimpleNumberValue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-statusClassID=1246974",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(21));
		assertThat(allRows.get(0).get(new Marque()).statusClassID.intValue(), is(1246974));
	}

	@Test
	public void testCreateDBQueryFromEncodedAcceptsSimpleNumberRange() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
		DBQuery query = DataModel.createDBQueryFromEncodedTablesPropertiesAndValues(
				database,
				"nz.co.gregs.dbvolution.example.CarCompany&nz.co.gregs.dbvolution.example.Marque-statusClassID=1246972...1246974",
				new ExampleEncodingInterpreter()
		);
		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(22));
		assertThat(allRows.get(0).get(new Marque()).statusClassID.intValue(), isOneOf(1246974, 1246972));
	}

	@Test
	public void testEncoding() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {

		final CarCompany carCompany = new CarCompany();
		carCompany.name.permittedValues("TOYOTA");
		Marque marque = new Marque();
		marque.name.permittedValues("TOYOTA");
		DBQuery query = database.getDBQuery(marque, carCompany);

		List<DBQueryRow> allRows = query.getAllRows();

		assertThat(allRows.size(), is(1));
		assertThat(allRows.get(0).get(new CarCompany()).name.stringValue(), is("TOYOTA"));
		assertThat(allRows.get(0).get(marque).name.stringValue(), isOneOf("TOYOTA"));

		final ExampleEncodingInterpreter encoder = new ExampleEncodingInterpreter();

		String encode = encoder.encode(allRows);
		String safeEncoded = encode.replaceAll("\\.00000", "").replaceAll(":56[.0]* [^ ]* 2013", ":56 2013");
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.CarCompany-name=TOYOTA"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=1"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-uidMarque=1"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-isUsedForTAFROs=False&"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-statusClassID=1246974&"));
		if (!(database instanceof OracleDB)) {
			// Oracle null/empty strings breaks this assertion
			assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-individualAllocationsAllowed=&"));
			assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-auto_created=&"));
			assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-pricingCodePrefix=&"));
		}
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-updateCount=0&"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-name=TOYOTA&"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-reservationsAllowed=Y&"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-creationDate=Mar 23 12:34:56 2013&"));
		assertThat(safeEncoded, Matchers.containsString("nz.co.gregs.dbvolution.example.Marque-carCompany=1"));

		final String encodedQuery = encoder.encode(allRows.get(0).get(new CarCompany()), marque);

		assertThat(encodedQuery, is("nz.co.gregs.dbvolution.example.CarCompany-name=TOYOTA&"
				+ "nz.co.gregs.dbvolution.example.CarCompany-uidCarCompany=1&"
				+ "nz.co.gregs.dbvolution.example.Marque"));

		query = DataModel
				.createDBQueryFromEncodedTablesPropertiesAndValues(database, encodedQuery,
						new ExampleEncodingInterpreter()
				);
		allRows = query.getAllRows();

		assertThat(allRows.size(), is(2));
		assertThat(allRows.get(0).get(new CarCompany()).name.stringValue(), is("TOYOTA"));
		assertThat(allRows.get(0).get(marque).name.stringValue(), isOneOf("TOYOTA", "HYUNDAI"));
		assertThat(allRows.get(1).get(marque).name.stringValue(), isOneOf("TOYOTA", "HYUNDAI"));

	}
}
