package nl.fews.verification.mongodb.web;

import graphql.GraphQLError;
import graphql.GraphqlErrorBuilder;
import graphql.schema.DataFetchingEnvironment;
import org.springframework.graphql.execution.DataFetcherExceptionResolverAdapter;
import org.springframework.stereotype.Component;

@Component
public class ExceptionComponent extends DataFetcherExceptionResolverAdapter {
	@Override
	protected GraphQLError resolveToSingleError(Throwable ex, DataFetchingEnvironment e){
		return GraphqlErrorBuilder.newError()
				.message(ex.getMessage())
				.path(e.getExecutionStepInfo().getPath())
				.location(e.getField().getSourceLocation())
				.build();
	}
}
