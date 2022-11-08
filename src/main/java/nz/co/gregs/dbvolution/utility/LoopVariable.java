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

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.Consumer;

/**
 * Implements a while/for loop combination.
 *
 * @author gregorygraham
 */
public class LoopVariable implements Serializable {

	private static final long serialVersionUID = 1L;

	public static LoopVariable withMaxAttempts(int size) {
		LoopVariable newLoop = LoopVariable.factory();
		newLoop.setMaxAttemptsAllowed(size);
		return newLoop;
	}
	public static LoopVariable withInfiniteLoopsPermitted(int size) {
		LoopVariable newLoop = LoopVariable.factory();
		newLoop.setInfiniteLoopPermitted();
		return newLoop;
	}

	private boolean needed = true;
	private int maxAttemptsAllowed = 1000;
	private boolean limitMaxAttempts = true;
	private transient final State state = new State();

	public static LoopVariable factory() {
		return new LoopVariable();
	}

	public static LoopVariable factory(int max) {
		LoopVariable loopVariable = new LoopVariable();
		loopVariable.setMaxAttemptsAllowed(max);
		return loopVariable;
	}

	/**
	 * Checks the whether the loop is still needed (that is {@link #done()} has
	 * not been called) and if the loop has exceeded the maximum attempts (if a
	 * max is defined).
	 *
	 * @return true if the loop is still needed.
	 */
	public boolean isNeeded() {
		if (limitMaxAttempts) {
			return needed && attempts() < maxAttemptsAllowed;
		} else {
			return needed;
		}
	}

	/**
	 * Checks the whether the loop is still needed (that is {@link #done()} has
	 * not been called) and if the loop has exceeded the maximum attempts (if a
	 * max is defined).
	 *
	 * @return true if the loop is no longer needed.
	 */
	public boolean isNotNeeded() {
		if (limitMaxAttempts) {
			return !needed || attempts() >= maxAttemptsAllowed;
		} else {
			return !needed;
		}
	}

	/**
	 * Synonym for {@link #isNotNeeded() }.
	 *
	 * @return true if the loop is no longer needed.
	 */
	public boolean hasHappened() {
		return isNotNeeded();
	}

	/**
	 * Synonym for {@link #isNeeded() }.
	 *
	 * @return true if the loop is still needed.
	 */
	public boolean hasNotHappened() {
		return isNeeded();
	}

	/**
	 * Informs the LoopVariable that the loop has been successful and is no longer
	 * needed.
	 * <p>
	 * This method is used to indicate that a loop that takes multiple attempts to
	 * complete one task, has successfully completed that task.</p>
	 */
	public void done() {
		needed = false;
	}

	/**
	 * Indicates that an attempt has been started.
	 *
	 * <p>
	 * This method is used to indicate that a loop that takes multiple attempts to
	 * complete one task, has started an attempt to complete that task. Each cvall
	 * of {@link #attempt() } counts towards the
	 * {@link #setMaxAttemptsAllowed(int) maximum attempts} if a maximum has been
	 * set.</p>
	 *
	 */
	public void attempt() {
		state.increaseTries();
	}

	/**
	 * The number of attempts recorded using {@link #attempt() }.
	 *
	 * @return the number of attempts started.
	 */
	public int attempts() {
		return state.getTries();
	}

	public Duration elapsedTime() {
		return state.elapsedTime();
	}

	/**
	 * Sets the maximum attempts allowed for this loop variable.
	 *
	 * <p>
	 * Maximum attempts will stop a correctly used LoopVariable after the maximum
	 * attempts by changing {@link #isNeeded() } to false</p>
	 *
	 * <p>
	 * Attempts are registered by calling {@link #attempt() } at the start of each
	 * loop.</p>
	 *
	 * @param maxAttemptsAllowed the number of attempts after which the loop will
	 * abort.
	 * @return this object with the configuration changed
	 */
	public LoopVariable setMaxAttemptsAllowed(int maxAttemptsAllowed) {
		if (maxAttemptsAllowed > 0) {
			limitMaxAttempts = true;
			this.maxAttemptsAllowed = maxAttemptsAllowed;
		}
		return this;
	}

	/**
	 * Removes the default limit from the LoopVariable.
	 *
	 * <p>
	 * By default {@link #isNeeded() } will return false after 1000 attempts. Use
	 * this method to remove the limit and permit infinite loops.</p>
	 *
	 * <p>
	 * Alternatively you can seta higher, or lower, limit with {@link #setMaxAttemptsAllowed(int)
	 * }.</p>
	 * @return this object with the configuration changed
	 */
	public LoopVariable setInfiniteLoopPermitted() {
		limitMaxAttempts = false;
		return this;
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
	 *		// Decide on a maximum number of loops
	 *		final int intendedAttempts = 10;
	 *
	 *		// Create the method to loop over
	 *		final Supplier&lt;Void&gt; action = () -&gt; {
	 *			// do your processing
	 *			// here
	 *
	 *			// check for terminating condition
	 *			if (successfullyCompleted()) {
	 *				// call done() on the LoopVariable to stop the loop
	 *				looper.done();
	 *			}
	 *			// return null as required by the Java spec
	 *			return null;
	 *		};
	 *
	 *		// Create the LoopVariable
	 *		LoopVariable looper = new LoopVariable();
	 *
	 *		// set the maximum number of loops (this is optional, but will default to 1000 anyway)
	 *		looper.setMaxAttemptsAllowed(intendedAttempts);
	 *
	 *		// loop over the action
	 *		looper.loop(action);
	 * </pre>
	 *
	 * @param action the action to perform within the loop
	 */
	public void loop(Supplier<Void> action) {
		Consumer<Integer> function = (index) -> action.get();
		loop(function);
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
	 *				() -&gt; {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				},
	 *				() -&gt; {
	 *					// Check for termination conditions here
	 *					return trueIfTaskCompletedOtherwiseFalse();
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action the action to perform with a loop
	 * @param test the test to check, if TRUE the loop will be terminated, if
	 * FALSE the loop will continue
	 */
	public void loop(Supplier<Void> action, Supplier<Boolean> test) {
		Consumer<Integer> function = (index) -> action.get();
		Function<Integer, Boolean> testFunction = (index) -> test.get();
		loop(function, testFunction);
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
	 *				() -&gt; {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action the action to perform with a loop
	 */
	public void loop(Consumer<Integer> action) {
		loop(action, (d) -> {
			return false;
		});
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
	 *				() -&gt; {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action the action to perform with a loop
	 * @return if an exception stops processing it is immediately returned
	 * @throws java.lang.Exception
	 */
	public Exception loop(Function<Integer, Exception> action) throws Exception {
		return loop(action, (d) -> {
			return false;
		});
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
	 *		looper.loop(
	 *				(index) -&gt; {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				},
	 *				(index) -&gt; {
	 *					// Check for termination conditions here
	 *					return trueIfTaskCompletedOtherwiseFalse();
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action the action to perform with a loop
	 * @param test the test to check, if TRUE the loop will be terminated, if
	 * FALSE the loop will continue
	 */
	public void loop(Consumer<Integer> action, Function<Integer, Boolean> test) {
		while (isNeeded()) {
			attempt();
			action.accept(getIndex());
			if (test.apply(getIndex())) {
				done();
			}
			increaseIndex();
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
	 *		looper.loop(
	 *				(index) -&gt; {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				},
	 *				(index) -&gt; {
	 *					// Check for termination conditions here
	 *					return trueIfTaskCompletedOtherwiseFalse();
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action the action to perform with a loop
	 * @param test the test to check, if TRUE the loop will be terminated, if
	 * FALSE the loop will continue
	 * @return if an exception occurs during processing it is immediately returned
	 */
	public Exception loop(Function<Integer, Exception> action, Function<Integer, Boolean> test) {
		while (isNeeded()) {
			attempt();
			Exception except = action.apply(getIndex());
			if (except!=null){
				return except;
			}
			if (test.apply(getIndex())) {
				done();
			}
			increaseIndex();
		}
		return null;
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
	 *		looper.loop(
	 *				(index) -&gt; {
	 *					// do your processing here
	 *
	 *					// return null as required by Java
	 *					return null;
	 *				},
	 *				(index) -&gt; {
	 *					// Check for termination conditions here
	 *					return trueIfTaskCompletedOtherwiseFalse();
	 *				},
	 *				(index) -&gt; {
	 *					// perform any post loop operations here
	 *					System.out.println("Completed loop after "+attempts()+" attempts");
	 *				}
	 *		);
	 * </pre>
	 *
	 * @param action the action to perform with a loop
	 * @param test the test to check, if TRUE the loop will be terminated, if
	 * FALSE the loop will continue
	 * @param completion the action to perform immediately after the loop
	 */
	public void loop(Consumer<Integer> action, Function<Integer, Boolean> test, Consumer<Integer> completion) {
		while (isNeeded()) {
			attempt();
			action.accept(getIndex());
			if (test.apply(getIndex())) {
				done();
			}
			increaseIndex();
		}
		completion.accept(getIndex());
	}

	public int getIndex() {
		return state.index();
	}

	private void increaseIndex() {
		state.increaseIndex();
	}

	public static class State {

		private int tries = 0;
		private int index = 0;
		private final Instant startTime = Instant.now();

		public State() {
		}

		public void increaseTries() {
			tries++;
		}

		public int getTries() {
			return tries;
		}

		public Instant getStartTime() {
			return startTime;
		}

		public Duration elapsedTime() {
			Duration duration = Duration.between(getStartTime(), Instant.now());
			return duration;
		}

		public int index() {
			return index;
		}

		public void increaseIndex() {
			index++;
		}
	}
}
