package nl.fews.verification.mongodb.generate.shared.execute;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Execute {
	private Execute(){}

	/**
	 * Executes a list of tasks using a thread pool.
	 *
	 * @param group list of tasks to execute
	 * @throws RuntimeException if an exception occurs during execution
	 */
	public static void execute(List<Object> group){
		var pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);
		try {
			List<Future<Object>> results = pool.invokeAll(group.stream().map(task -> (Callable<Object>) () -> {
				((IExecute) task).execute();
				return null;
			}).toList());
			for (Future<Object> x : results) {
				x.get();
			}
		}
		catch (InterruptedException | ExecutionException ex) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(ex);
		}
		finally {
			pool.shutdown();
		}
	}

	/**
	 * Executes a list of tasks using a thread pool.
	 *
	 * @param group list of tasks to execute
	 * @throws RuntimeException if an exception occurs during execution
	 */
	public static void executeSerially(List<Object> group){
		try {
			group.forEach(task -> ((IExecute) task).execute());
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
