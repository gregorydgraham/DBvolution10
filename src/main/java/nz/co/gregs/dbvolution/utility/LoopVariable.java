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

/**
 *
 * @author gregorygraham
 */
public class LoopVariable {

	private boolean toggle = true;
	private int tries = 0;
	private int maxAttemptsAllowed = 1000;
	private boolean limitMaxAttempts = true;
	
	public static LoopVariable factory(){
		return new LoopVariable();
	}

	public boolean isNeeded() {
		if (limitMaxAttempts) {
			return toggle && attempts() < maxAttemptsAllowed;
		} else {
			return toggle;
		}
	}

	public boolean isNotNeeded() {
		if (limitMaxAttempts) {
			return !toggle || attempts() >= maxAttemptsAllowed;
		} else {
			return !toggle;
		}
	}

	public boolean hasHappened() {
		return isNotNeeded();
	}

	public boolean hasNotHappened() {
		return isNeeded();
	}

	public void done() {
		toggle = false;
	}

	public void attempt() {
		tries++;
	}

	public int attempts() {
		return tries;
	}

	public void setMaxAttemptsAllowed(int maxAttemptsAllowed) {
		if (maxAttemptsAllowed > 0) {
			limitMaxAttempts = true;
			this.maxAttemptsAllowed = maxAttemptsAllowed;
		}
	}

	public void setInfiniteLoopPermitted() {
		limitMaxAttempts = false;
	}

	/**
	 * Performs action until {@link #done() } has been called.
	 *
	 * <p>
	 * This is a re-implementation of the while loop mechanism. Probably not as
	 * good as the actual while loop but it was fun to do.</p>
	 *
	 * <p>
	 * recommended use:</p>
	 * <pre>
	 *		// Create the variable for check for the end of the loop
	 *		final int intendedAttempts = 10;
	 *		// Create the LoopVariable
	 *		LoopVariable looper = new LoopVariable();
	 *		// Create the method to loop over
	 *		final Supplier&lt;Void&gt; action = () -> {
	 *			// do your processing
	 *			// here
	 *
	 *			// check for terminating condition
	 *			if (looper.attempts() >= intendedAttempts) {
	 *				// call done() on the LoopVariable to stop the loop
	 *				looper.done();
	 *			}
	 *			// return null as required by the Java spec
	 *			return null;
	 *		};
	 *		// loop over the action
	 *		looper.loop(action);
	 * </pre>
	 *
	 * @param action
	 */
	public void loop(Supplier<Void> action) {
		while (isNeeded()) {
			attempt();
			action.get();
		}
	}

	/**
	 * Performs action until {@link #done() } has been called, or {@link #attempt()
	 * } has been called maxAttempts times.
	 *
	 * <p>
	 * This is a re-implementation of the while loop mechanism. Probably not as
	 * good as the actual while loop but it was fun to do.</p>
	 *
	 * <pre>
	 *		// Create the LoopVariable
	 *		LoopVariable looper = new LoopVariable();
	 *
	 *		// Call loop() with maximum number of attempts to try
	 *		looper.loop(100, () -> {
	 *
	 *			// Perform your actions here
	 *
	 *			// check for the termination conditions
	 *			if (looper.attempts() >= 10) {
	 *				// call done() to terminate the loop
	 *				looper.done();
	 *			}
	 *
	 *			// return NULL because Java requires us to
	 *			return null;
	 *		});
	 * </pre>
	 *
	 * @param maxAttempts
	 * @param action
	 */
	public void loop(int maxAttempts, Supplier<Void> action) {
		while (isNeeded() && attempts() < maxAttempts) {
			attempt();
			action.get();
		}
	}

	/**
	 * Performs action until test returns true.
	 *
	 * <p>
	 * This is a re-implementation of the while loop mechanism. Probably not as
	 * good as the actual while loop but it was fun to do.</p>
	 *
	 * <pre>
	 *		LoopVariable looper = new LoopVariable();
	 *		final int intendedAttempts = 10;
	 *		looper.loop(
	 *				() -> {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				},
	 *				() -> {
	 *					// Check for termination conditions here
	 *					return looper.attempts() >= intendedAttempts;
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action
	 * @param test
	 */
	public void loop(Supplier<Void> action, Supplier<Boolean> test) {
		while (isNeeded()) {
			attempt();
			action.get();
			if (test.get()) {
				done();
			}
		}
	}
}
