/*
 * Copyright 2023 Gregory Graham.
 *
 * Commercial licenses are available, please contact info@gregs.co.nz for details.
 * 
 * This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. 
 * To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/4.0/ 
 * or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.
 * 
 * You are free to:
 *     Share - copy and redistribute the material in any medium or format
 *     Adapt - remix, transform, and build upon the material
 * 
 *     The licensor cannot revoke these freedoms as long as you follow the license terms.               
 *     Under the following terms:
 *                 
 *         Attribution - 
 *             You must give appropriate credit, provide a link to the license, and indicate if changes were made. 
 *             You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.
 *         NonCommercial - 
 *             You may not use the material for commercial purposes.
 *         ShareAlike - 
 *             If you remix, transform, or build upon the material, 
 *             you must distribute your contributions under the same license as the original.
 *         No additional restrictions - 
 *             You may not apply legal terms or technological measures that legally restrict others from doing anything the 
 *             license permits.
 * 
 * Check the Creative Commons website for any details, legalese, and updates.
 */
package nz.co.gregs.dbvolution.actions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import nz.co.gregs.dbvolution.DBRow;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.QueryIntention;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.AccidentalBlankQueryException;
import nz.co.gregs.dbvolution.exceptions.UnableToInstantiateDBRowSubclassException;
import nz.co.gregs.dbvolution.internal.properties.PropertyWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author gregorygraham
 */
public class DBCreateTable extends DBAction {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(DBDropTable.class);

	private final boolean includeForeignKeyClauses;
	ArrayList<PropertyWrapper<?, ?, ?>> pkFields = new ArrayList<>();
	ArrayList<PropertyWrapper<?, ?, ?>> spatial2DFields = new ArrayList<>();

	public <R extends DBRow> DBCreateTable(R row, boolean includeForeignKeyClauses) {
		super(row, QueryIntention.CREATE_TABLE);
		this.includeForeignKeyClauses = includeForeignKeyClauses;
	}

	@Override
	protected DBActionList getRevertDBActionList() {
		DBActionList reverts = new DBActionList();
		reverts.add(new DBDropTable(getRow()));
		return reverts;
	}

	@Override
	public List<String> getSQLStatements(DBDatabase db) {
		List<String> result = new ArrayList<>(1);
		DBRow newTableRow = getRow();

		List<String> sqlString = getSQLForCreateTable(db, newTableRow, includeForeignKeyClauses);
		result.addAll(sqlString);

		return result;
	}

	@Override
	public DBActionList execute(DBDatabase db) throws SQLException {

		return execute2(db);
	}

	@Override
	protected DBActionList prepareActionList(DBDatabase db) throws AccidentalBlankQueryException, SQLException, UnableToInstantiateDBRowSubclassException {
		DBRow table = getRow();
		final DBCreateTable newAction = new DBCreateTable(table, includeForeignKeyClauses);
		DBActionList actions = new DBActionList(newAction);
		return actions;
	}

	@Override
	protected void prepareRollbackData(DBDatabase db, DBActionList actions) {
		// no data required
	}

	private synchronized List<String> getSQLForCreateTable(DBDatabase db, DBRow newTableRow, boolean includeForeignKeyClauses) {
		DBDefinition definition = db.getDefinition();
		StringBuilder sqlScript = new StringBuilder();
		ArrayList<String> sqlList = new ArrayList<>();

		String lineSeparator = System.getProperty("line.separator");
		// table name

		sqlScript.append(definition.getCreateTableStart()).append(definition.formatTableName(newTableRow)).append(definition.getCreateTableColumnsStart()).append(lineSeparator);

		// columns
		String sep = "";
		String nextSep = definition.getCreateTableColumnsSeparator();
		var fields = newTableRow.getColumnPropertyWrappers();
		List<String> fkClauses = new ArrayList<>();
		pkFields.clear();
		spatial2DFields.clear();
		for (var field : fields) {
			if (field.isColumn() && !field.getQueryableDatatype().hasColumnExpression()) {
				String colName = field.columnName();
				sqlScript
						.append(sep)
						.append(definition.formatColumnName(colName))
						.append(definition.getCreateTableColumnsNameAndTypeSeparator())
						.append(definition.getSQLTypeAndModifiersOfDBDatatype(field));
				sep = nextSep + lineSeparator;

				if (field.isPrimaryKey()) {
					pkFields.add(field);
				}
				if (field.isSpatial2DType()) {
					spatial2DFields.add(field);
				}
				String fkClause = definition.getForeignKeyClauseForCreateTable(field);
				if (!fkClause.isEmpty()) {
					fkClauses.add(fkClause);
				}
			}
		}

		if (includeForeignKeyClauses) {
			for (String fkClause : fkClauses) {
				sqlScript.append(sep).append(fkClause);
				sep = nextSep + lineSeparator;
			}
		}

		// primary keys
		if (definition.prefersTrailingPrimaryKeyDefinition()) {
			String pkStart = lineSeparator + definition.getCreateTablePrimaryKeyClauseStart();
			String pkMiddle = definition.getCreateTablePrimaryKeyClauseMiddle();
			String pkEnd = definition.getCreateTablePrimaryKeyClauseEnd() + lineSeparator;
			String pkSep = pkStart;
			for (var field : pkFields) {
				sqlScript.append(pkSep).append(definition.formatColumnName(field.columnName()));
				pkSep = pkMiddle;
			}
			if (!pkSep.equalsIgnoreCase(pkStart)) {
				sqlScript.append(pkEnd);
			}
		}

		//finish
		sqlScript.append(definition.getCreateTableColumnsEnd()).append(lineSeparator).append(definition.endSQLStatement());
		sqlList.add(sqlScript.toString());

		//Oracle style trigger based auto-increment keys
		if (definition.prefersTriggerBasedIdentities() && pkFields.size() == 1) {
			List<String> triggerBasedIdentitySQL = definition.getTriggerBasedIdentitySQL(db, definition.formatTableName(newTableRow), definition.formatColumnName(pkFields.get(0).columnName()));
			for (String sql : triggerBasedIdentitySQL) {
				sqlList.add(sql);
			}
		}

		if (definition.requiresSpatial2DIndexes() && spatial2DFields.size() > 0) {
			List<String> triggerBasedIdentitySQL = definition.getSpatial2DIndexSQL(db, definition.formatTableName(newTableRow), definition.formatColumnName(spatial2DFields.get(0).columnName()));
			for (String sql : triggerBasedIdentitySQL) {
				sqlList.add(sql);
			}
		}

		return sqlList;
	}
}
