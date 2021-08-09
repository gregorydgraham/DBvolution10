/*
 * Copyright 2020 Gregory Graham.
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
package nz.co.gregs.dbvolution.databases.settingsbuilders;

import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.definitions.DBDefinition;

/**
 * Settings builder for all databases with an associated vendor.
 * 
 * <p>That is all databases that aren't DBvolution clusters.</p>
 *
 * @author gregorygraham
 * @param <SELF>
 * @param <DATABASE>
 */
public abstract class AbstractVendorSettingsBuilder<SELF extends AbstractVendorSettingsBuilder<SELF, DATABASE>, DATABASE extends DBDatabase> 
		extends AbstractSettingsBuilder<SELF, DATABASE> 
		implements VendorSettingsBuilder<SELF, DATABASE> {

	private String driverName = getDefaultDriverName();
	private DBDefinition definition = getDefaultDefinition();


	@SuppressWarnings("unchecked")
	@Override
	public final SELF setDriverName(String driverName) {
		this.driverName = driverName;
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final SELF setDefinition(DBDefinition defn) {
		this.definition = defn;
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final String getDriverName() {
		return (driverName==null?getDefaultDriverName():driverName);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final DBDefinition getDefinition() {
		return (definition==null?getDefaultDefinition():definition);
	}

}
