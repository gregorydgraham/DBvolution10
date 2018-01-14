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
package nz.co.gregs.dbvolution.results;

/**
 * A range result has a simple linear distribution.
 *
 * <p>
 * Good examples are the number line, dictionaries, and the time line. Based on
 * those examples numbers, strings, and dates are RangeResults.</p>
 *
 * <p>
 * Spatial values are not range comparable as they exist in 2 or more dimensions
 * and can't be ordered consistently on a line.</p>
 *
 * <p>
 * Booleans are not range comparable as there are only 3 values (true, false,
 * and null) and there is no commonly agreed ordering.</p>
 *
 * @author gregorygraham
 * @param <T> a base type that has a commonly understood linear distribution
 * like Number, String, or Date
 */
public interface RangeResult<T> extends InResult<T>{

}
