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
import java.util.stream.StreamSupport;

@Controller
public class Study {
	@QueryMapping
	public Document studyById(@Argument String _id, DataFetchingEnvironment e){
		return Mongo.findOne("Study", new Document("_id", new ObjectId(_id)), Conversion.getProjection(e));
	}

	@QueryMapping
	public Document studyByName(@Argument String name, DataFetchingEnvironment e){
		return Mongo.findOne("Study", new Document("Name", name), Conversion.getProjection(e));
	}

	@QueryMapping
	public List<Document> studyN(DataFetchingEnvironment e){
		return StreamSupport.stream(Mongo.find("Study", new Document(), Conversion.getProjection(e)).sort(new Document("Name", 1)).spliterator(), false).toList();
	}
	
	@MutationMapping
	public String createStudy(
			@Argument String name,
			@Argument String observed,
			@Argument List<String> forecasts,
			@Argument List<String> seasonalities,
			@Argument String _class,
			@Argument String locationAttributes,
			@Argument String forecastStartMonth,
			@Argument String forecastEndMonth,
			@Argument String time,
			@Argument String value,
			@Argument String normal,
			@Argument String cube,
			@Argument boolean active,
			@Argument int reprocessDays,
			@Argument int maxLeadTimeMinutes){
		return Mongo.insertOne("Study",
				new Document("Name", name)
				.append("Observed", observed)
				.append("Forecasts", forecasts)
				.append("Seasonalities", seasonalities)
				.append("Class", _class)
				.append("LocationAttributes", locationAttributes)
				.append("ForecastStartMonth", forecastStartMonth)
				.append("ForecastEndMonth", forecastEndMonth)
				.append("Time", time)
				.append("Value", value)
				.append("Normal", normal)
				.append("Cube", cube)
				.append("Active", active)
				.append("ReprocessDays", reprocessDays)
				.append("MaxLeadTimeMinutes", maxLeadTimeMinutes)
		).getInsertedId().asObjectId().getValue().toString();
	}

	@MutationMapping
	public Long updateStudy(
			@Argument String _id,
			@Argument String name,
			@Argument String observed,
			@Argument List<String> forecasts,
			@Argument List<String> seasonalities,
			@Argument String _class,
			@Argument String locationAttributes,
			@Argument String forecastStartMonth,
			@Argument String forecastEndMonth,
			@Argument String time,
			@Argument String value,
			@Argument String normal,
			@Argument String cube,
			@Argument boolean active,
			@Argument int reprocessDays,
			@Argument int maxLeadTimeMinutes){
		return Mongo.updateOne("Study", new Document("_id", new ObjectId(_id)), new Document("$set",
				new Document("Name", name)
				.append("Observed", observed)
				.append("Forecasts", forecasts)
				.append("Seasonalities", seasonalities)
				.append("Class", _class)
				.append("LocationAttributes", locationAttributes)
				.append("ForecastStartMonth", forecastStartMonth)
				.append("ForecastEndMonth", forecastEndMonth)
				.append("Time", time)
				.append("Value", value)
				.append("Normal", normal)
				.append("Cube", cube)
				.append("Active", active)
				.append("ReprocessDays", reprocessDays)
				.append("MaxLeadTimeMinutes", maxLeadTimeMinutes)
		)).getModifiedCount();
	}

	@MutationMapping
	public Long deleteStudy(@Argument String _id){
		return Mongo.deleteOne("Study", new Document("_id", new ObjectId(_id))).getDeletedCount();
	}
}
