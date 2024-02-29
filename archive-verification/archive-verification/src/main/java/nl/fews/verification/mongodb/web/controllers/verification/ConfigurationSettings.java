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
public class ConfigurationSettings {
	@QueryMapping
	public Document configurationSettingsById(@Argument String _id, DataFetchingEnvironment e){
		return  Mongo.findOne("configuration.Settings", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document configurationSettingsByEnvironment(@Argument String environment, DataFetchingEnvironment e){
		return  Mongo.findOne("configuration.Settings", new Document("Environment", environment), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> configurationSettingsN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("configuration.Settings", new Document(), Conversion.getProjection(e)).spliterator(), false).collect(Collectors.toList());
	}

	@MutationMapping
	public String createConfigurationSettings(@Argument Map<String, Object> document){
		return Mongo.insertOne("configuration.Settings", new Document(document)).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateConfigurationSettings(@Argument String _id, @Argument Map<String, Object> document){
		return Mongo.updateOne("configuration.Settings", new Document("_id", new ObjectId(_id)), new Document(document)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteConfigurationSettings(@Argument String _id){
		return Mongo.deleteOne("configuration.Settings", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
