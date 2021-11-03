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

import java.util.function.Supplier;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author gregorygraham
 */
public class LoopVariableTest {

	public LoopVariableTest() {
	}

	@Test
	public void testLoop() {
		LoopVariable looper = new LoopVariable();
		final int intendedAttempts = 10;
		looper.loop(() -> {
			// do your processing
			// here
			// check for terminating condition
			if (looper.attempts() >= intendedAttempts) {
				looper.done();
			}
			// return null as required by the Java spec
			return null;
		});
		assertThat(looper.attempts(), is(intendedAttempts));
	}

	@Test
	public void testLoopUsedWronglyStopsAt1000Attempts() {
		LoopVariable looper = new LoopVariable();
		looper.loop(() -> {
			// do your processing
			// here

			// return null as required by the Java spec
			return null;
		});
		assertThat(looper.attempts(), is(1000));
	}

	@Test
	public void testLoopWithSupplierVariable() {
		final int intendedAttempts = 10;
		// Create the LoopVariable
		LoopVariable looper = new LoopVariable();
		// Create the method to loop over
		final Supplier<Void> action = () -> {
			// do your processing
			// here
			// check for terminating condition
			if (looper.attempts() >= intendedAttempts) {
				// call done() on the LoopVariable to stop the loop
				looper.done();
			}
			// return null as required by the Java spec
			return null;
		};
		// loop over the action
		looper.loop(action);
		assertThat(looper.attempts(), is(intendedAttempts));
	}

	@Test
	public void testLoopWithTest() {
		LoopVariable looper = new LoopVariable();
		final int intendedAttempts = 10;
		looper.loop(
				() -> {
					return null;
				},
				() -> {
					return looper.attempts() >= intendedAttempts;
				}
		);
		assertThat(looper.attempts(), is(intendedAttempts));
	}

	@Test
	public void testLoopStoppedByExceedingMaxAttemptsAllowed() {
		LoopVariable looper = new LoopVariable();
		looper.setMaxAttemptsAllowed(20);
		looper.loop(() -> {
			return null;
		});
		assertThat(looper.attempts(), is(20));
	}
}
