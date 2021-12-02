/*
 * Copyright 2021 Gregory Graham.
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
package nz.co.gregs.dbvolution.utility;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.prefs.BackingStoreException;
import java.util.prefs.InvalidPreferencesFormatException;
import java.util.prefs.NodeChangeListener;
import java.util.prefs.PreferenceChangeListener;

/**
 *
 * @author gregorygraham
 */
public class PreferencesImproved {

	private final java.util.prefs.Preferences prefs;

	public static PreferencesImproved userNodeForPackage(Class<?> c) {
		return new PreferencesImproved(java.util.prefs.Preferences.userNodeForPackage(c));
	}

	public static PreferencesImproved systemNodeForPackage(Class<?> c) {
		return new PreferencesImproved(java.util.prefs.Preferences.systemNodeForPackage(c));
	}

	public static PreferencesImproved userRoot() {
		return new PreferencesImproved(java.util.prefs.Preferences.userRoot());
	}

	public static PreferencesImproved systemRoot() {
		return new PreferencesImproved(java.util.prefs.Preferences.systemRoot());
	}

	private PreferencesImproved(java.util.prefs.Preferences actualPreferences) {
		prefs = actualPreferences;
	}

	public void put(String key, String value) {
		try {
			prefs.put(key, value);
		} catch (IllegalArgumentException tooLong) {
			List<String> blocks = StringInBlocks.withBlockSize(java.util.prefs.Preferences.MAX_VALUE_LENGTH).withString(value).getBlocks();
			for (int i = 0; i < blocks.size(); i++) {
				prefs.put(key + "[" + i + "]", blocks.get(i));
			}
		}
	}

	public String get(String key, String defaultValue) {
		String defaultString = defaultValue == null ? "" : defaultValue;
		String simpleGot = prefs.get(key, defaultString);
		if (!defaultString.equals(simpleGot)) {
			return simpleGot;
		} else {
			List<String> foundStrings = new ArrayList<>(0);
			LoopVariable findStopper = new LoopVariable();
			while (findStopper.hasNotHappened()) {
				simpleGot = prefs.get(key + "[" + findStopper.attempts() + "]", defaultString);
				findStopper.attempt();
				if (defaultString.equals(simpleGot)) {
					findStopper.done();
				} else {
					foundStrings.add(simpleGot);
				}
			}
			if (foundStrings.isEmpty()) {
				return defaultString;
			} else {
				StringBuilder str = new StringBuilder();
				for (String foundString : foundStrings) {
					str.append(foundString);
				}
				final String returnValue = str.toString();
				return returnValue;
			}
		}
	}

	public void remove(String key) {
		try {
			prefs.remove(key);
		} catch (Exception tooLong) {
			LoopVariable loop = new LoopVariable();
			while (loop.isNeeded()) {
				final String node = key + "[" + loop.attempts() + "]";
				try {
					if (prefs.nodeExists(node)) {
						prefs.remove(node);
						loop.attempt();
					} else {
						loop.done();
					}
				} catch (Exception ex) {
					loop.done();
				}
			}
		}
	}

	public void clear() throws BackingStoreException {
		prefs.clear();
	}

	public void putInt(String string, int i) {
		prefs.putInt(string, i);
	}

	public int getInt(String string, int i) {
		return prefs.getInt(string, i);
	}

	public void putLong(String string, long l) {
		prefs.putLong(string, l);
	}

	public long getLong(String string, long l) {
		return prefs.getLong(string, l);
	}

	public void putBoolean(String string, boolean bln) {
		prefs.putBoolean(string, bln);
	}

	public boolean getBoolean(String string, boolean bln) {
		return prefs.getBoolean(string, bln);
	}

	public void putFloat(String string, float f) {
		prefs.putFloat(string, f);
	}

	public float getFloat(String string, float f) {
		return prefs.getFloat(string, f);
	}

	public void putDouble(String string, double d) {
		prefs.putDouble(string, d);
	}

	public double getDouble(String string, double d) {
		return prefs.getDouble(string, d);
	}

	public void putByteArray(String string, byte[] bytes) {
		prefs.putByteArray(string, bytes);
	}

	public byte[] getByteArray(String string, byte[] bytes) {
		return prefs.getByteArray(string, bytes);
	}

	public String[] keys() throws BackingStoreException {
		return prefs.keys();
	}

	public String[] childrenNames() throws BackingStoreException {
		return prefs.childrenNames();
	}

	public PreferencesImproved parent() {
		return new PreferencesImproved(prefs.parent());
	}

	public PreferencesImproved node(String string) {
		return new PreferencesImproved(prefs.node(string));
	}

	public boolean nodeExists(String string) throws BackingStoreException {
		return prefs.nodeExists(string);
	}

	public void removeNode() throws BackingStoreException {
		prefs.removeNode();
	}

	public String name() {
		return prefs.name();
	}

	public String absolutePath() {
		return prefs.absolutePath();
	}

	public boolean isUserNode() {
		return prefs.isUserNode();
	}

	@Override
	public String toString() {
		return prefs.toString();
	}

	public void flush() throws BackingStoreException {
		prefs.flush();
	}

	public void sync() throws BackingStoreException {
		prefs.sync();
	}

	public void addPreferenceChangeListener(PreferenceChangeListener pcl) {
		prefs.addPreferenceChangeListener(pcl);
	}

	public void removePreferenceChangeListener(PreferenceChangeListener pcl) {
		prefs.removePreferenceChangeListener(pcl);
	}

	public void addNodeChangeListener(NodeChangeListener ncl) {
		prefs.addNodeChangeListener(ncl);
	}

	public void removeNodeChangeListener(NodeChangeListener ncl) {
		prefs.removeNodeChangeListener(ncl);
	}

	public void exportNode(OutputStream out) throws IOException, BackingStoreException {
		prefs.exportNode(out);
	}

	public void exportSubtree(OutputStream out) throws IOException, BackingStoreException {
		prefs.exportSubtree(out);
	}

	public static void importPreferences(InputStream is) throws IOException, InvalidPreferencesFormatException {
		java.util.prefs.Preferences.importPreferences(is);
	}
}
