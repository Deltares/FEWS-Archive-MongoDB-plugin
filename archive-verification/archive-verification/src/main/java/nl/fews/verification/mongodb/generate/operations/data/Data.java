package nl.fews.verification.mongodb.generate.operations.data;

import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.generate.shared.execute.Execute;
import nl.fews.verification.mongodb.generate.shared.graph.Graph;
import nl.fews.verification.mongodb.shared.settings.Settings;
import org.bson.Document;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.StreamSupport;

public class Data {
	private Data(){}

	public static void execute() {
		boolean processData = Settings.get("processData", Boolean.class);
		if (!processData)
			return;

		boolean parallel = Settings.get("parallel", Boolean.class);
		if (parallel) {
			var pool = Executors.newFixedThreadPool(Settings.get("dataThreads", Integer.class));
			try {
				List<Future<Object>> results = pool.invokeAll(StreamSupport.stream(Mongo.find("Study", new Document("Active", true)).spliterator(), false).map(study -> (Callable<Object>) () -> {
					Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Data.class, new Object[]{study.getString("Name")})).forEach(Execute::execute);
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
		else {
			Mongo.find("Study", new Document("Active", true)).forEach(study ->
				Graph.getDirectedAcyclicGraphGroups(Graph.getDirectedAcyclicGraph(Data.class, new Object[]{study.getString("Name")})).forEach(Execute::execute)
			);
		}
	}
}
