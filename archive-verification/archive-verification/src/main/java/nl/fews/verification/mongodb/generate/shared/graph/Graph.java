package nl.fews.verification.mongodb.generate.shared.graph;

import nl.fews.verification.mongodb.generate.interfaces.IExecute;
import nl.fews.verification.mongodb.generate.interfaces.IPredecessor;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.reflections.Reflections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuppressWarnings({"unchecked"})
public class Graph {

	private Graph(){}

	/**
	 * This method takes a DirectedAcyclicGraph and returns a list of groups where each group
	 * contains vertices that do not have any ancestors in the graph. The method uses a clone of
	 * the input graph to ensure that the original graph is not modified.
	 *
	 * @param directedAcyclicGraph the directed acyclic graph
	 * @return a list of groups, where each group contains vertices with no ancestors
	 */
	public static List<List<Object>> getDirectedAcyclicGraphGroups(DirectedAcyclicGraph<Object, DefaultEdge> directedAcyclicGraph){
		directedAcyclicGraph = (DirectedAcyclicGraph<Object, DefaultEdge>)directedAcyclicGraph.clone();
		List<List<Object>> directedAcyclicGraphGroups = new ArrayList<>();

		while(!directedAcyclicGraph.vertexSet().isEmpty()){
			List<Object> group = new ArrayList<>();
			for (Object v: directedAcyclicGraph.vertexSet()){
				if(directedAcyclicGraph.getAncestors(v).isEmpty()){
					group.add(v);
				}
			}
			directedAcyclicGraphGroups.add(group);
			for(Object v: group) {
				directedAcyclicGraph.removeVertex(v);
			}
		}

		return directedAcyclicGraphGroups;
	}

	/**
	 * Retrieves a directed acyclic graph (DAG) based on a given class.
	 *
	 * @param clazz the class for which the DAG is created
	 * @return the directed acyclic graph
	 */
	public static DirectedAcyclicGraph<Object, DefaultEdge> getDirectedAcyclicGraph(Class<?> clazz) {
		return Graph.getDirectedAcyclicGraph(clazz, new Class<?>[]{});
	}

	/**
	 * Retrieves a directed acyclic graph (DAG) based on a given class and parameters.
	 *
	 * @param clazz       the class for which the DAG is created
	 * @param parameters  an array of objects representing the parameters for the class constructor
	 * @return the directed acyclic graph
	 */
	public static DirectedAcyclicGraph<Object, DefaultEdge> getDirectedAcyclicGraph(Class<?> clazz, Object[] parameters){
		Class<?>[] parameterTypes = Arrays.stream(parameters).map(Object::getClass).toArray(Class<?>[]::new);

		Map<String, Object> r = new Reflections(clazz.getPackage().getName()).getSubTypesOf(IExecute.class).stream().collect(Collectors.toMap(
				Class::getName,
				a -> {
					try {
						return a.getConstructor(parameterTypes).newInstance(parameters);
					}
					catch (Exception ex){
						throw new RuntimeException(ex);
					}
				}));

		DirectedAcyclicGraph<Object, DefaultEdge> directedAcyclicGraph = new DirectedAcyclicGraph<>(DefaultEdge.class);

		r.forEach((k, v) -> {
			try {
				directedAcyclicGraph.addVertex(v);
				Arrays.stream(((IPredecessor) v).getPredecessors()).forEach(p -> {
					directedAcyclicGraph.addVertex(r.get(p));
					directedAcyclicGraph.addEdge(r.get(p), v);
				});
			}
			catch (Exception ex){
				throw new RuntimeException(ex);
			}
		});

		return directedAcyclicGraph;
	}
}
