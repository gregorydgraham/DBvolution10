/*
 * Copyright 2018 gregorygraham.
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
package nz.co.gregs.dbvolution.internal.query;

import java.util.Objects;
import nz.co.gregs.dbvolution.DBRow;

/**
 *
 * @author gregorygraham
 */
public class DBRowClass {
	
	private final DBRow table;
	
	public DBRowClass(DBRow table) {
		this.table = table;
	}
	
	public String getSimpleName() {
		return this.table.getClass().getSimpleName();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (super.equals(obj)) {
			return true;
		} else {
			if (obj instanceof DBRowClass) {
				DBRowClass other = (DBRowClass) obj;
				if (this.table == null && other.table == null) {
					return true;
				} else if (this.table == null || other.table == null) {
					return false;
				} else if (!this.table.getClass().equals(other.table.getClass())) {
					return false;
				} else {
					return this.table.getTableName().equals(other.table.getTableName())
							&& this.table.getSchemaName().equals(other.table.getSchemaName())
							&& this.table.getTableNameOrVariantIdentifier().equals(other.table.getTableNameOrVariantIdentifier());
				}
			} else {
				return false;
			}
		}
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 41 * hash + Objects.hashCode(this.table.getClass());
		hash = 43*hash + Objects.hashCode(this.table.getTableName());
		hash = 47*hash + Objects.hashCode(this.table.getSchemaName());
		hash = 53*hash + Objects.hashCode(this.table.getTableNameOrVariantIdentifier());
		return hash;
	}
	
}
