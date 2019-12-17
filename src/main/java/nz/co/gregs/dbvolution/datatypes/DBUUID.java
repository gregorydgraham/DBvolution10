/*
 * Copyright 2019 Gregory Graham.
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
package nz.co.gregs.dbvolution.datatypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import nz.co.gregs.dbvolution.columns.ColumnProvider;
import nz.co.gregs.dbvolution.columns.UUIDColumn;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.exceptions.IncorrectRowProviderInstanceSuppliedException;
import nz.co.gregs.dbvolution.expressions.StringExpression;
import nz.co.gregs.dbvolution.expressions.UUIDExpression;
import nz.co.gregs.dbvolution.query.RowDefinition;
import nz.co.gregs.dbvolution.results.UUIDResult;

/**
 * The quick and easy way to add and use universally unique identifiers (UUID)
 * in your application.
 *
 * <p>
 * Please check Java's {@link UUID UUID documentation} for details of the UUID
 * implementation.</p>
 *
 * @author gregorygraham
 */
public class DBUUID extends QueryableDatatype<UUID> implements UUIDResult {

	public DBUUID() {
	}

	public DBUUID(UUID value) {
		super(value);
	}

	public DBUUID(UUIDResult uuid) {
		super(uuid);
	}

	@Override
	public void setValue(UUID uuid) {
		super.setValue(uuid);
	}

	public void setValue(String uuid) {
		this.setValue(UUID.fromString(uuid));
	}

	@Override
	protected void setValueFromStandardStringEncoding(String uuid) {
		setValue(uuid);
	}

	@Override
	public String getSQLDatatype() {
		return "VARCHAR(64)";
	}

	@Override
	protected String formatValueForSQLStatement(DBDefinition defn) {
		String unsafeValue = getLiteralValue().toString();
		return defn.beginStringValue() + defn.safeString(unsafeValue) + defn.endStringValue();
	}

	@Override
	protected UUID getFromResultSet(DBDefinition database, ResultSet resultSet, String fullColumnName) throws SQLException {
		String gotString = resultSet.getString(fullColumnName);
		if (resultSet.wasNull() || gotString == null) {
			return null;
		} else {
			return UUID.fromString(gotString);
		}
	}

	@Override
	public ColumnProvider getColumn(RowDefinition row) throws IncorrectRowProviderInstanceSuppliedException {
		return new UUIDColumn(row, this);
	}

	@Override
	public boolean isAggregator() {
		return false;
	}

	@Override
	public DBUUID copy() {
		return (DBUUID) super.copy();
	}

	@Override
	public boolean getIncludesNull() {
		return getValue() == null;
	}

	@Override
	public StringExpression stringResult() {
		return new UUIDExpression(this).stringResult();
	}

	public boolean isEmpty() {
		var val = getLiteralValue();
		if (val == null) {
			return true;
		} else {
			return val.toString().isEmpty();
		}
	}

	/**
	 * Sets the value to a new random UUID.
	 *
	 * <p>
	 * Uses {@link UUID#randomUUID() UUID.randomUUID}.</p>
	 *
	 */
	public void setValueToNewRandomUUID() {
		this.setValue(UUID.randomUUID());
	}

	/**
	 * Set the value to a new UUID using the specified data.
	 *
	 * <p>
	 * mostSigBits is used for the most significant 64 bits of the UUID and
	 * leastSigBits becomes the least significant 64 bits of the UUID.</p>
	 *
	 * <p>
	 * Please consult {@link UUID} for more details.</p>
	 *
	 * @param most
	 * @param least
	 */
	public void setValueToNewUUID(Long most, Long least) {
		this.setValue(new UUID(most, least));
	}

	/**
	 * Sets the value to a named UUID using
	 * {@link UUID#nameUUIDFromBytes(byte[]) the standard UUID factory method}.
	 *
	 * @param name
	 * @see UUID
	 *
	 */
	public void setValueToNamedUUIDFromBytes(byte[] name) {
		this.setValue(UUID.nameUUIDFromBytes(name));
	}

}
