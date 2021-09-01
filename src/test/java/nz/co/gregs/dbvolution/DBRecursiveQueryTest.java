/*
 * Copyright 2014 Gregory Graham.
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

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.exceptions.ColumnProvidedMustBeAForeignKey;
import nz.co.gregs.dbvolution.exceptions.ForeignKeyDoesNotReferenceATableInTheQuery;
import nz.co.gregs.dbvolution.exceptions.ForeignKeyIsNotRecursiveException;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.annotations.*;
import nz.co.gregs.dbvolution.datatypes.*;
import nz.co.gregs.dbvolution.generic.AbstractTest;
import nz.co.gregs.dbvolution.query.TreeNode;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

/**
 *
 * <p style="color: #F90;">Support DBvolution at
 * <a href="http://patreon.com/dbvolution" target=new>Patreon</a></p>
 *
 * @author Gregory Graham
 */
//@Ignore
public class DBRecursiveQueryTest extends AbstractTest {

	final Parts wing = new Parts(null, "wing");
	Parts aileron;
	Parts lever;
	Parts screw;

	final PartsStringKey wingString = new PartsStringKey("wing", null, "wing");
	PartsStringKey aileronString;
	PartsStringKey leverString;
	PartsStringKey screwString;

	final PartsWithoutTableName wingWithout = new PartsWithoutTableName(null, "wing");
	PartsWithoutTableName aileronWithout;

	public DBRecursiveQueryTest(Object testIterationName, Object db) {
		super(testIterationName, db);
	}

	@Override
	public void setup(DBDatabase database) throws Exception {
		super.setup(database);
		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new Parts());
		database.createTable(new Parts());
		database.insert(wing);
		aileron = new Parts(wing.partID.intValue(), "aileron");
		database.insert(aileron);
		lever = new Parts(aileron.partID.intValue(), "lever");
		screw = new Parts(aileron.partID.intValue(), "screw");
		database.insert(lever);
		database.insert(screw);

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new PartsWithoutTableName());
		database.createTable(new PartsWithoutTableName());
		database.insert(wingWithout);
		aileronWithout = new PartsWithoutTableName(wing.partID.intValue(), "aileron");
		database.insert(aileronWithout);
		database.insert(new PartsWithoutTableName(aileron.partID.intValue(), "lever"));
		database.insert(new PartsWithoutTableName(aileron.partID.intValue(), "screw"));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new CompletePart());
		database.createTable(new CompletePart());
		database.insert(new CompletePart(aileron.partID.intValue(), "Aileron"));
		database.insert(new CompletePart(wing.partID.intValue(), "Wing"));

		database.preventDroppingOfTables(false);
		database.dropTableNoExceptions(new PartsStringKey());
		database.createTable(new PartsStringKey());
		database.insert(wingString);
		aileronString = new PartsStringKey("aileronid", wingString.partID.stringValue(), "aileron");
		database.insert(aileronString);
		leverString = new PartsStringKey("leverid", aileronString.partID.stringValue(), "lever");
		screwString = new PartsStringKey("screwid", aileronString.partID.stringValue(), "screw");
		database.insert(leverString);
		database.insert(screwString);
	}

	@Test
	public void descendSimpleTree() throws SQLException {
		Parts aileronID = new Parts();
		aileronID.partID.permittedValues(aileron.partID.intValue());
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, aileronID.column(aileronID.subPartOf));
		@SuppressWarnings("unchecked")
		List<Parts> componentsOfTheAileron = recursive.getDescendants();

		assertThat(componentsOfTheAileron.size(), is(3));
		assertThat(componentsOfTheAileron.get(0).name.stringValue(), is("aileron"));
		assertThat(componentsOfTheAileron.get(1).name.stringValue(), anyOf(is("screw"), is("lever")));
		assertThat(componentsOfTheAileron.get(2).name.stringValue(), anyOf(is("screw"), is("lever")));
	}

	@Test
	public void descendSimpleTreeUsingDBDatabaseConvenienceMethod() throws SQLException {
		Parts aileronID = new Parts();
		aileronID.partID.permittedValues(aileron.partID.intValue());
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<Parts> recursive = database.getDBRecursiveQuery(findTheAileronQuery, aileronID.column(aileronID.subPartOf), aileronID);
		List<Parts> componentsOfTheAileron = recursive.getDescendants();

		assertThat(componentsOfTheAileron.size(), is(3));
		assertThat(componentsOfTheAileron.get(0).name.stringValue(), is("aileron"));
		assertThat(componentsOfTheAileron.get(1).name.stringValue(), anyOf(is("screw"), is("lever")));
		assertThat(componentsOfTheAileron.get(2).name.stringValue(), anyOf(is("screw"), is("lever")));
	}

	@Test
	public void descendSimpleTreeUsingDBQueryConvenienceMethod() throws SQLException {
		Parts aileronID = new Parts();
		aileronID.partID.permittedValues(aileron.partID.intValue());
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<Parts> recursive = findTheAileronQuery.getDBRecursiveQuery(aileronID.column(aileronID.subPartOf));
		List<Parts> componentsOfTheAileron = recursive.getDescendants();

		assertThat(componentsOfTheAileron.size(), is(3));
		assertThat(componentsOfTheAileron.get(0).name.stringValue(), is("aileron"));
		assertThat(componentsOfTheAileron.get(1).name.stringValue(), anyOf(is("screw"), is("lever")));
		assertThat(componentsOfTheAileron.get(2).name.stringValue(), anyOf(is("screw"), is("lever")));
	}

	@Test
	public void ascendSimpleTree() throws SQLException {
		Parts aileronID = new Parts();
		aileronID.partID.permittedValues(aileron.partID.intValue());
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, aileronID.column(aileronID.subPartOf));
		@SuppressWarnings("unchecked")
		List<Parts> componentsOfTheAileron = recursive.getAncestors();

		assertThat(componentsOfTheAileron.size(), is(2));
		assertThat(componentsOfTheAileron.get(0).name.stringValue(), is("aileron"));
		assertThat(componentsOfTheAileron.get(1).name.stringValue(), is("wing"));
	}

	@Test
	public void descendSimpleTreeWithoutTableName() throws SQLException {
		PartsWithoutTableName aileronID = new PartsWithoutTableName();
		aileronID.partID.permittedValues(aileronWithout.partID.intValue());
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<PartsWithoutTableName> recursive = new DBRecursiveQuery<PartsWithoutTableName>(findTheAileronQuery, aileronID.column(aileronID.subPartOf));
		@SuppressWarnings("unchecked")
		List<PartsWithoutTableName> componentsOfTheAileron
				= recursive.getDescendants();

		assertThat(componentsOfTheAileron.size(), is(3));
		for (PartsWithoutTableName component : componentsOfTheAileron) {
			final DBString firstName = component.name;
			assertThat(firstName.stringValue(),
					anyOf(is("aileron"), is("lever"), is("screw")));
		}
	}

	@Test
	public void ascendSimpleTreeWithoutTableName() throws SQLException {
		PartsWithoutTableName aileronID = new PartsWithoutTableName();
		aileronID.partID.permittedValues(aileronWithout.partID.intValue());
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<PartsWithoutTableName> recursive = new DBRecursiveQuery<PartsWithoutTableName>(findTheAileronQuery, aileronID.column(aileronID.subPartOf));
		@SuppressWarnings("unchecked")
		List<PartsWithoutTableName> componentsOfTheAileron
				= recursive.getAncestors();

		assertThat(componentsOfTheAileron.size(), is(2));
	}

	@Test
	public void descendTreeFrom2TableQuery() throws SQLException {
		Parts part = new Parts();
		CompletePart aileronID = new CompletePart();
		aileronID.name.permittedValues("Aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(new Parts(), aileronID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, part.column(part.subPartOf));
		List<Parts> componentsOfTheAileron
				= recursive.getDescendants();

		assertThat(componentsOfTheAileron.size(), is(3));
		assertThat(componentsOfTheAileron.get(0).name.stringValue(), is("aileron"));
		assertThat(componentsOfTheAileron.get(1).name.stringValue(), anyOf(is("screw"), is("lever")));
		assertThat(componentsOfTheAileron.get(2).name.stringValue(), anyOf(is("screw"), is("lever")));
	}

	@Test
	public void ascendTreeFrom2TableQuery() throws SQLException {
		Parts part = new Parts();
		CompletePart aileronID = new CompletePart();
		aileronID.name.permittedValues("Aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(new Parts(), aileronID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, part.column(part.subPartOf));
		List<Parts> componentsOfTheAileron
				= recursive.getAncestors();

		assertThat(componentsOfTheAileron.size(), is(2));
	}

	@Test
	public void descendTreeFrom2TableQueryToPathsList() throws SQLException {
		Parts part = new Parts();
		CompletePart aileronID = new CompletePart();
		aileronID.name.permittedValues("Aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(new Parts(), aileronID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, part.column(part.subPartOf));
		List<Parts> componentsOfTheAileron = recursive.getDescendants();

		assertThat(componentsOfTheAileron.size(), is(3));
		assertThat(componentsOfTheAileron.get(0).name.stringValue(), is("aileron"));
		assertThat(componentsOfTheAileron.get(1).name.stringValue(), anyOf(is("screw"), is("lever")));
		assertThat(componentsOfTheAileron.get(2).name.stringValue(), anyOf(is("screw"), is("lever")));
	}

	@Test
	public void ascendTreeFrom2TableQueryToPathsList() throws SQLException {
		Parts part = new Parts();
		CompletePart aileronID = new CompletePart();
		aileronID.name.permittedValues("Aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(new Parts(), aileronID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, part.column(part.subPartOf));
		List<Parts> componentsOfTheAileron
				= recursive.getAncestors();

		assertThat(componentsOfTheAileron.size(), is(2));
	}

	@Test
	public void getPathToRoot() throws SQLException {
		Parts part = new Parts();
		CompletePart aileronID = new CompletePart();
		aileronID.name.permittedValues("Aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(new Parts(), aileronID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, part.column(part.subPartOf));
		TreeNode<Parts> pathToTheWing
				= recursive.getPathsToRoot().get(0);

		assertThat(pathToTheWing.getData().name.stringValue(), is("aileron"));
		assertThat(pathToTheWing.getParent().getData().name.stringValue(), is("wing"));
	}

	@Test
	public void getPathsToRoot() throws SQLException {
		Parts part = new Parts();
		part.name.permittedValues("lever", "screw");
		final DBQuery findTheAileronQuery = database.getDBQuery(part);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, part.column(part.subPartOf));
		List<TreeNode<Parts>> pathToTheWing
				= recursive.getPathsToRoot();

		assertThat(pathToTheWing.get(0).getData().name.stringValue(), anyOf(is("lever"), is("screw")));
		assertThat(pathToTheWing.get(1).getData().name.stringValue(), anyOf(is("lever"), is("screw")));
		assertThat(pathToTheWing.get(0).getParent().getData().name.stringValue(), is("aileron"));
		assertThat(pathToTheWing.get(1).getParent().getData().name.stringValue(), is("aileron"));
		assertThat(pathToTheWing.get(0).getParent().getParent().getData().name.stringValue(), is("wing"));
	}

	@Test
	public void getTreeFromRoot() throws SQLException {
		Parts part = new Parts();
		CompletePart wingID = new CompletePart();
		wingID.name.permittedValues("Wing");
		final DBQuery findTheAileronQuery = database.getDBQuery(new Parts(), wingID);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(findTheAileronQuery, part.column(part.subPartOf));
		TreeNode<Parts> treeFromWing
				= recursive.getTrees().get(0);

		assertThat(treeFromWing.getData().name.stringValue(), is("wing"));
		final TreeNode<Parts> aileronProbably = treeFromWing.getChildren().get(0);
		assertThat(aileronProbably.getData().name.stringValue(), is("aileron"));
		assertThat(aileronProbably.getChildren().get(0).getData().name.stringValue(), is("lever"));
		assertThat(aileronProbably.getChildren().get(1).getData().name.stringValue(), is("screw"));
	}

	@Test
	public void getTreesFromRoot() throws SQLException {
		Parts part = new Parts();
		part.name.permittedValues("lever", "screw");
		DBQuery baseQuery = database.getDBQuery(part);

		DBRecursiveQuery<Parts> recursive = new DBRecursiveQuery<Parts>(baseQuery, part.column(part.subPartOf));
		List<TreeNode<Parts>> trees
				= recursive.getTrees();

		assertThat(trees.get(0).getData().name.stringValue(), anyOf(is("lever"), is("screw")));
		assertThat(trees.get(1).getData().name.stringValue(), anyOf(is("lever"), is("screw")));
		assertThat(trees.get(0).getChildren().size(), is(0));
		assertThat(trees.get(1).getChildren().size(), is(0));

		part = new Parts();
		part.name.permittedValues("lever", "screw", "wing");
		baseQuery = database.getDBQuery(part);

		recursive = new DBRecursiveQuery<Parts>(baseQuery, part.column(part.subPartOf));
		trees = recursive.getTrees();

		assertThat(trees.size(), is(3));
		final TreeNode<Parts> wingProbably = trees.get(0);
		assertThat(wingProbably.getData().name.stringValue(), is("wing"));
		final List<TreeNode<Parts>> wingChildren = trees.get(0).getChildren();
		assertThat(wingChildren.size(), is(1));
		final TreeNode<Parts> aileronProbably = wingChildren.get(0);
		assertThat(aileronProbably.getData().name.stringValue(), is("aileron"));
		final List<TreeNode<Parts>> aileronChildren = aileronProbably.getChildren();

		assertThat(aileronChildren.size(), is(2));
		assertThat(aileronChildren.get(0).getData().name.stringValue(), anyOf(is("lever"), is("screw")));
		assertThat(aileronChildren.get(1).getData().name.stringValue(), anyOf(is("lever"), is("screw")));
		
		TreeNode<Parts> oneTree = trees.get(1);
		assertThat(oneTree.getData().name.stringValue(), is("lever"));
		List<TreeNode<Parts>> children = oneTree.getChildren();
		assertThat(children.size(), is(0));
		
		oneTree = trees.get(2);
		assertThat(oneTree.getData().name.stringValue(), is("screw"));
		children = oneTree.getChildren();
		assertThat(children.size(), is(0));
		
	}

	@Test
	public void checkEverythingWorksForStringIDs() throws SQLException {
		PartsStringKey aileronID = new PartsStringKey();
		aileronID.name.permittedValues("aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<PartsStringKey> recursive = new DBRecursiveQuery<PartsStringKey>(findTheAileronQuery, aileronID.column(aileronID.subPartOf));
		List<PartsStringKey> pathToTheWing
				= recursive.getAncestors();

		assertThat(pathToTheWing.size(), is(2));
		assertThat(pathToTheWing.get(0).name.stringValue(), is("aileron"));
		assertThat(pathToTheWing.get(1).name.stringValue(), is("wing"));
	}

	@Test(expected = ForeignKeyDoesNotReferenceATableInTheQuery.class)
	public void checkForeignKeyInvolvesQueryTablesException() throws SQLException {
		Parts part = new Parts();
		PartsStringKey aileronID = new PartsStringKey();
		aileronID.name.permittedValues("aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<PartsStringKey> recursive = new DBRecursiveQuery<PartsStringKey>(
				findTheAileronQuery, part.column(part.subPartOf));
	}

	@Test(expected = ColumnProvidedMustBeAForeignKey.class)
	public void checkColumnIsNotAForeignKeyException() throws SQLException {
		PartsStringKey aileronID = new PartsStringKey();
		aileronID.name.permittedValues("aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID);

		DBRecursiveQuery<PartsStringKey> recursive = new DBRecursiveQuery<PartsStringKey>(
				findTheAileronQuery, aileronID.column(aileronID.name));
	}

	@Test(expected = ForeignKeyIsNotRecursiveException.class)
	public void checkForeignKeyIsRecursiveException() throws SQLException {
		PartsStringKey aileronID = new PartsStringKey();
		aileronID.name.permittedValues("aileron");
		final DBQuery findTheAileronQuery = database.getDBQuery(aileronID, new Parts());

		DBRecursiveQuery<PartsStringKey> recursive = new DBRecursiveQuery<PartsStringKey>(
				findTheAileronQuery, aileronID.column(aileronID.fkToParts));
	}

	@Test 
	public void copesWithLoopInPathToRoot() throws SQLException {
		Parts tail = new Parts();
		tail.name.setValue("Tail");
		database.insert(tail);
		Parts fuselage = new Parts();
		fuselage.name.setValue("Fuselage");
		database.insert(fuselage);
		tail.subPartOf.setValue(fuselage.partID);
		fuselage.subPartOf.setValue(tail.partID);
		database.update(tail, fuselage);

		Parts example = new Parts();
		example.name.permittedValues("Fuselage");
		final DBQuery query = database.getDBQuery(example);

		DBRecursiveQuery<Parts> recursive 
				= new DBRecursiveQuery<Parts>(query, example.column(example.subPartOf));
		List<TreeNode<Parts>> pathsToRoot = recursive.getPathsToRoot();
		assertThat(pathsToRoot.size(), is(1));
		TreeNode<Parts> path = pathsToRoot.get(0);

		assertThat(path.getData().name.stringValue(), is("Fuselage"));
		assertThat(path.getParent().getData().name.stringValue(), is("Tail"));
		database.delete(tail, fuselage);
	}

	@Test 
	public void copesWithLoopInTrees() throws SQLException {
		Parts nose = new Parts();
		nose.name.setValue("Nose");
		database.insert(nose);
		Parts cockpit = new Parts();
		cockpit.name.setValue("Cockpit");
		database.insert(cockpit);
		nose.subPartOf.setValue(cockpit.partID);
		cockpit.subPartOf.setValue(nose.partID);
		database.update(nose, cockpit);

		Parts example = new Parts();
		example.name.permittedValues("Cockpit");
		final DBQuery query = database.getDBQuery(example);

		DBRecursiveQuery<Parts> recursive 
				= new DBRecursiveQuery<Parts>(query, example.column(example.subPartOf));
		List<TreeNode<Parts>> trees = recursive.getTrees();
		assertThat(trees.size(), is(1));
		TreeNode<Parts> path = trees.get(0);

		assertThat(path.getData().name.stringValue(), is("Cockpit"));
		assertThat(path.getParent().getData().name.stringValue(), is("Nose"));
		database.delete(nose, cockpit);
	}

	@DBTableName("parts")
	public static class Parts extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("part_id")
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger partID = new DBInteger();

		@DBColumn
		@DBForeignKey(Parts.ParentPart.class)
		public DBInteger subPartOf = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		public Parts() {
			super();
		}

		public Parts(Integer parentPartID, String name) {
			super();
			this.subPartOf.setValue(parentPartID);
			this.name.setValue(name);
		}

		public static class ParentPart extends Parts {

			private static final long serialVersionUID = 1L;

			public ParentPart() {
				super();
			}
		}
	}

	@DBTableName("parts_string_key")
	public static class PartsStringKey extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("part_id")
		@DBPrimaryKey
		public DBString partID = new DBString();

		@DBColumn
		@DBForeignKey(PartsStringKey.ParentPart.class)
		public DBString subPartOf = new DBString();

		@DBColumn
		public DBString name = new DBString();

		@DBColumn
		@DBForeignKey(Parts.class)
		public DBInteger fkToParts = new DBInteger();

		public PartsStringKey() {
			super();
		}

		public PartsStringKey(String id, String parentPartID, String name) {
			super();
			this.partID.setValue(id);
			this.subPartOf.setValue(parentPartID);
			this.name.setValue(name);
		}

		public static class ParentPart extends PartsStringKey {

			private static final long serialVersionUID = 1L;

			public ParentPart() {
				super();
			}
		}
	}

	@DBTableName("complete_parts")
	public static class CompletePart extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger completePartID = new DBInteger();

		@DBColumn
		@DBForeignKey(Parts.class)
		public DBInteger partID = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		public CompletePart() {
			super();
		}

		public CompletePart(Integer parentPartID, String name) {
			super();
			this.partID.setValue(parentPartID);
			this.name.setValue(name);
		}

	}

	// TODO Investigate need for "part_id"
	public static class PartsWithoutTableName extends DBRow {

		private static final long serialVersionUID = 1L;

		@DBColumn("part_id")
		@DBPrimaryKey
		@DBAutoIncrement
		public DBInteger partID = new DBInteger();

		@DBColumn
		@DBForeignKey(ParentPart.class)
		public DBInteger subPartOf = new DBInteger();

		@DBColumn
		public DBString name = new DBString();

		public PartsWithoutTableName() {
			super();
		}

		public PartsWithoutTableName(Integer parentPartID, String name) {
			super();
			this.subPartOf.setValue(parentPartID);
			this.name.setValue(name);
		}

		public static class ParentPart extends PartsWithoutTableName {

			private static final long serialVersionUID = 1L;

			public ParentPart() {
				super();
			}
		}
	}
}
