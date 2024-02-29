package nl.fews.verification.mongodb.web.controllers.verification;

import graphql.schema.DataFetchingEnvironment;
import nl.fews.verification.mongodb.shared.database.Mongo;
import nl.fews.verification.mongodb.web.shared.conversion.Conversion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Controller
public class Seasonality {
	@QueryMapping
	public Document seasonalityById(@Argument String _id, DataFetchingEnvironment e){
		return  Mongo.findOne("Seasonality", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document seasonalityByName(@Argument String name, DataFetchingEnvironment e){
		return  Mongo.findOne("Seasonality", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> seasonalityN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("Seasonality", new Document(), Conversion.getProjection(e)).spliterator(), false).collect(Collectors.toList());
	}
	
	@MutationMapping
	public String createSeasonality(@Argument Map<String, Object> document){
		return Mongo.insertOne("Seasonality", new Document(document)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateSeasonality(@Argument String _id, @Argument Map<String, Object> document){
		return Mongo.updateOne("Seasonality", new Document("_id", new ObjectId(_id)), new Document(document)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteSeasonality(@Argument String _id){
		return Mongo.deleteOne("Seasonality", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
