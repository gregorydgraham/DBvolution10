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
package nz.co.gregs.dbvolution.expressions;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;
import nz.co.gregs.dbvolution.datatypes.DBString;
import nz.co.gregs.dbvolution.datatypes.DBUUID;
import nz.co.gregs.dbvolution.datatypes.QueryableDatatype;
import nz.co.gregs.dbvolution.results.AnyResult;
import nz.co.gregs.dbvolution.results.InComparable;
import nz.co.gregs.dbvolution.results.StringResult;
import nz.co.gregs.dbvolution.results.UUIDResult;

public class UUIDExpression extends InExpression<UUID, UUIDResult, DBUUID> implements InComparable<UUID, UUIDResult>, UUIDResult {

	private static final long serialVersionUID = 1L;

	public UUIDExpression() {
		super();
	}

	public UUIDExpression(StringResult stringVariable) {
		super(stringVariable);
	}

	public UUIDExpression(UUIDResult stringVariable) {
		super(stringVariable);
	}

	protected UUIDExpression(AnyResult<?> stringVariable) {
		super(stringVariable);
	}

	public UUIDExpression(String stringVariable) {
		super(new DBString(stringVariable));
	}

	public UUIDExpression(DBString stringVariable) {
		super(stringVariable);
	}

	public UUIDExpression(DBUUID stringVariable) {
		super(stringVariable);
	}

	private UUIDExpression(UUID value) {
		super(new DBUUID(value));
	}

	@Override
	public BooleanExpression isInCollection(Collection<UUIDResult> values) {
		List<StringExpression> collected
				= values
						.stream()
						.map((t) -> t.stringResult())
						.collect(Collectors.toList());
		return this.stringResult().isIn(collected.toArray(new StringResult[]{}));
	}

	@Override
	public BooleanExpression isNotInCollection(Collection<UUIDResult> values) {
		List<StringExpression> collected
				= values
						.stream()
						.map((t) -> t.stringResult())
						.collect(Collectors.toList());
		return this.stringResult().isNotIn(collected.toArray(new StringResult[]{}));
	}

	@Override
	public UUIDResult expression(UUID value) {
		return new UUIDExpression(value);
	}

	@Override
	public UUIDResult expression(UUIDResult value) {
		return new UUIDExpression(value);
	}

	@Override
	public UUIDResult expression(DBUUID value) {
		return new UUIDExpression(value);
	}

	@Override
	public DBUUID asExpressionColumn() {
		return new DBUUID(this);
	}

	@Override
	public QueryableDatatype<?> getQueryableDatatypeForExpressionValue() {
		return new DBUUID();
	}

	@Override
	public UUIDExpression copy() {
		return new UUIDExpression(this.getInnerResult());
	}

	@Override
	public BooleanExpression is(UUIDResult anotherInstance) {
		return this.stringResult().is(anotherInstance.stringResult());
	}

	@Override
	public BooleanExpression isNot(UUIDResult anotherInstance) {
		return this.stringResult().isNot(anotherInstance.stringResult());
	}

	@Override
	public BooleanExpression is(UUID anotherInstance) {
		return this.stringResult().is(
				expression(anotherInstance).stringResult()
		);
	}

	@Override
	public BooleanExpression isNot(UUID anotherInstance) {
		return this
				.stringResult()
				.isNot(anotherInstance.toString());
	}

	@Override
	public StringExpression stringResult() {
		return new StringResultFunction(this);
	}

	private class StringResultFunction extends StringExpression {

		private final static long serialVersionUID = 1l;
		private final UUIDExpression only;

		public StringResultFunction(UUIDExpression only) {
			this.only = only;
		}

		@Override
		public String toSQLString(DBDefinition db) {
			return only.toSQLString(db);
		}

		@Override
		public boolean getIncludesNull() {
			return only.getIncludesNull();
		}

		@Override
		public StringResultFunction copy() {
			return new StringResultFunction(
					only == null ? null : only.copy());
		}
	}

}
