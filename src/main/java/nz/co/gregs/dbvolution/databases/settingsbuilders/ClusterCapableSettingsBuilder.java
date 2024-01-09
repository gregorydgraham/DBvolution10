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
package nz.co.gregs.dbvolution.databases.settingsbuilders;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import nz.co.gregs.dbvolution.databases.DBDatabase;
import nz.co.gregs.dbvolution.databases.DatabaseConnectionSettings;
import nz.co.gregs.separatedstring.SeparatedString;
import nz.co.gregs.separatedstring.SeparatedStringBuilder;

/**
 *
 * @author gregorygraham
 * @param <SELF> the class of the object returned by most methods, this should be the Class of "this"
 * @param <DATABASE> the class returned by {@link #getDBDatabase}
 */
public interface ClusterCapableSettingsBuilder<SELF extends ClusterCapableSettingsBuilder<SELF, DATABASE>, DATABASE extends DBDatabase> extends SettingsBuilder<SELF, DATABASE> {

	List<DatabaseConnectionSettings> getClusterHosts();

	@SuppressWarnings("unchecked")
	default SELF setClusterHosts(DatabaseConnectionSettings... hosts) {
		setClusterHosts(Arrays.asList(hosts));
		return (SELF) this;
	}

	@SuppressWarnings("unchecked")
	default SELF addClusterHosts(DatabaseConnectionSettings... hosts) {
		addClusterHosts(Arrays.asList(hosts));
		return (SELF) this;
	}
	
	@SuppressWarnings("unchecked")
	default SELF setClusterHosts(List<DatabaseConnectionSettings> hosts) {
		this.getClusterHosts().clear();
		this.getClusterHosts().addAll(hosts);
		return (SELF) this;
	}
	
	@SuppressWarnings("unchecked")
	default SELF addClusterHosts(List<DatabaseConnectionSettings> hosts) {
		this.getClusterHosts().addAll(hosts);
		return (SELF) this;
	}
	
	public default String encodeClusterHosts(List<DatabaseConnectionSettings> hosts) {
		SeparatedString sep = SeparatedStringBuilder.forSeparator(",");
		sep.addAll(
				hosts.stream()
						.map(this::encodeHost)
						.collect(Collectors.toList())
		);
		return sep.toString();
	}
}
