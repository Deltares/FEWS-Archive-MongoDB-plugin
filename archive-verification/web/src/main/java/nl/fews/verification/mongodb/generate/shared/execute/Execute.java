package nl.fews.verification.mongodb.generate.shared.execute;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.shared.settings.Settings;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
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
		try {
			ExecutorService pool = Executors.newFixedThreadPool(Settings.get("threads"));


			class ExecutionCallable implements Callable<Void> {
				private final Object task;

				public ExecutionCallable(Object task) {
					this.task = task;
				}

				@Override
				public Void call() {
					try {
						((IExecute) task).execute();
						return null;
					}
					catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			}

			List<Future<Void>> results = pool.invokeAll(group.stream().map(ExecutionCallable::new).toList());
			for (Future<Void> x : results) {
				x.get();
			}
			pool.shutdown();
		}
		catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
}
