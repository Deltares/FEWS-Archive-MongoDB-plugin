package nl.fews.verification.mongodb.web.controllers.verification;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.shared.settings.Settings;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

@Controller
public class Forecast {
	@QueryMapping
	public Document forecastById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("Forecast", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document forecastByName(@Argument String name, DataFetchingEnvironment e){
		return Mongo.findOne("Forecast", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> forecastN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("Forecast", new Document(), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).toList();
	}

	@QueryMapping
	public List<Document> forecastTest(@Argument String collection, @Argument List<Map<String, Object>> filters, DataFetchingEnvironment e){
		var database = Settings.get("archiveDb", String.class);
		return filters.stream().map(f -> {
			try{
				var r = Mongo.aggregate(database, collection, List.of(new Document("$match", f.get("Filter")), new Document("$limit", 1))).maxTime(10, TimeUnit.SECONDS).first();
				return new Document("FilterName", f.get("FilterName")).append("Success", String.format("%b", r != null));
			}
			catch (Exception ex){
				return new Document("FilterName", f.get("FilterName")).append("Success", ex.getMessage());
			}
		}).toList();
	}
	
	@MutationMapping
	public String createForecast(@Argument String name, @Argument String forecastName, @Argument String collection, @Argument List<Map<String, Object>> filters){
		return Mongo.insertOne("Forecast", new Document("Name", name).append("ForecastName", forecastName).append("Collection", collection).append("Filters", filters)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateForecast(@Argument String _id, @Argument String name, @Argument String forecastName, @Argument String collection, @Argument List<Map<String, Object>> filters){
		return Mongo.updateOne("Forecast", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Name", name).append("ForecastName", forecastName).append("Collection", collection).append("Filters", filters))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteForecast(@Argument String _id){
		return Mongo.deleteOne("Forecast", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
