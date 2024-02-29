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
import java.util.stream.StreamSupport;

@Controller
public class Class {
	@QueryMapping
	public Document classById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("Class", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document classByName(@Argument String name, DataFetchingEnvironment e){
		return Mongo.findOne("Class", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> classN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("Class", new Document(), Conversion.getProjection(e)).spliterator(), false).toList();
	}

	@MutationMapping
	public String createClass(@Argument String name, @Argument List<Map<String, Object>> locations){
		return Mongo.insertOne("Class", new Document("Name", name).append("Locations", locations)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateClass(@Argument String _id, @Argument String name, @Argument List<Map<String, Object>> locations){
		return Mongo.updateOne("Class", new Document("_id", new ObjectId(_id)), new Document("$set", new Document("Name", name).append("Locations", locations))).getModifiedCount();
	}

	@MutationMapping
	public Long deleteClass(@Argument String _id){
		return Mongo.deleteOne("Class", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
