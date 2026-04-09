db.getCollection("view.ExternalForecastingScalarTimeSeries").drop()
db.createView("view.ExternalForecastingScalarTimeSeries", "ExternalForecastingScalarTimeSeries", [
  {
    "$unwind": "$timeseries"
  },
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "ForecastTime": "$forecastTime",
      "LocalEndTime": "$localEndTime",
      "LocalForecastTime": "$localForecastTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "StartTime": "$startTime",
      "TimeSeriesType": "$timeSeriesType",
      "Comment": "$timeseries.c",
      "DisplayValue": "$timeseries.dv",
      "Flag": "$timeseries.f",
      "LocalTime": "$timeseries.lt",
      "Time": "$timeseries.t",
      "Value": "$timeseries.v"
    }
  }
]);
db.getCollection("view.ExternalForecastingScalarTimeSeriesHeader").drop()
db.createView("view.ExternalForecastingScalarTimeSeriesHeader", "ExternalForecastingScalarTimeSeries", [
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "ForecastTime": "$forecastTime",
      "LocalEndTime": "$localEndTime",
      "LocalForecastTime": "$localForecastTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "StartTime": "$startTime",
      "TimeSeriesType": "$timeSeriesType"
    }
  }
]);
db.getCollection("view.ExternalHistoricalScalarTimeSeries").drop()
db.createView("view.ExternalHistoricalScalarTimeSeries", "ExternalHistoricalScalarTimeSeries", [
  {
    "$unwind": "$timeseries"
  },
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Bucket": "$bucket",
      "BucketSize": "$bucketSize",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "LocalEndTime": "$localEndTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "StartTime": "$startTime",
      "TimeSeriesType": "$timeSeriesType",
      "Comment": "$timeseries.c",
      "DisplayValue": "$timeseries.dv",
      "Flag": "$timeseries.f",
      "LocalTime": "$timeseries.lt",
      "Time": "$timeseries.t",
      "Value": "$timeseries.v"
    }
  }
]);
db.getCollection("view.ExternalHistoricalScalarTimeSeriesHeader").drop()
db.createView("view.ExternalHistoricalScalarTimeSeriesHeader", "ExternalHistoricalScalarTimeSeries", [
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Bucket": "$bucket",
      "BucketSize": "$bucketSize",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "LocalEndTime": "$localEndTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "StartTime": "$startTime",
      "TimeSeriesType": "$timeSeriesType"
    }
  }
]);
db.getCollection("view.SimulatedForecastingScalarTimeSeries").drop()
db.createView("view.SimulatedForecastingScalarTimeSeries", "SimulatedForecastingScalarTimeSeries", [
  {
    "$unwind": "$timeseries"
  },
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "ForecastTime": "$forecastTime",
      "LocalEndTime": "$localEndTime",
      "LocalForecastTime": "$localForecastTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataApprovedTime": "$metaData.approvedTime",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "RunInfoConfigRevisionNumber": "$runInfo.configRevisionNumber",
      "RunInfoDispatchTime": "$runInfo.dispatchTime",
      "RunInfoMcId": "$runInfo.mcId",
      "RunInfoTaskRunId": "$runInfo.taskRunId",
      "RunInfoTime0": "$runInfo.time0",
      "RunInfoUserId": "$runInfo.userId",
      "RunInfoWorkflowId": "$runInfo.workflowId",
      "StartTime": "$startTime",
      "TaskRunId": "$taskRunId",
      "TimeSeriesType": "$timeSeriesType",
      "Comment": "$timeseries.c",
      "DisplayValue": "$timeseries.dv",
      "Flag": "$timeseries.f",
      "LocalTime": "$timeseries.lt",
      "Time": "$timeseries.t",
      "Value": "$timeseries.v"
    }
  }
]);
db.getCollection("view.SimulatedForecastingScalarTimeSeriesHeader").drop()
db.createView("view.SimulatedForecastingScalarTimeSeriesHeader", "SimulatedForecastingScalarTimeSeries", [
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "ForecastTime": "$forecastTime",
      "LocalEndTime": "$localEndTime",
      "LocalForecastTime": "$localForecastTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataApprovedTime": "$metaData.approvedTime",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "RunInfoConfigRevisionNumber": "$runInfo.configRevisionNumber",
      "RunInfoDispatchTime": "$runInfo.dispatchTime",
      "RunInfoMcId": "$runInfo.mcId",
      "RunInfoTaskRunId": "$runInfo.taskRunId",
      "RunInfoTime0": "$runInfo.time0",
      "RunInfoUserId": "$runInfo.userId",
      "RunInfoWorkflowId": "$runInfo.workflowId",
      "StartTime": "$startTime",
      "TaskRunId": "$taskRunId",
      "TimeSeriesType": "$timeSeriesType"
    }
  }
]);
db.getCollection("view.SimulatedHistoricalScalarTimeSeries").drop()
db.createView("view.SimulatedHistoricalScalarTimeSeries", "SimulatedHistoricalScalarTimeSeries", [
  {
    "$unwind": "$timeseries"
  },
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "ForecastTime": "$forecastTime",
      "LocalEndTime": "$localEndTime",
      "LocalForecastTime": "$localForecastTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataApprovedTime": "$metaData.approvedTime",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "RunInfoConfigRevisionNumber": "$runInfo.configRevisionNumber",
      "RunInfoDispatchTime": "$runInfo.dispatchTime",
      "RunInfoMcId": "$runInfo.mcId",
      "RunInfoTaskRunId": "$runInfo.taskRunId",
      "RunInfoTime0": "$runInfo.time0",
      "RunInfoUserId": "$runInfo.userId",
      "RunInfoWorkflowId": "$runInfo.workflowId",
      "StartTime": "$startTime",
      "TaskRunId": "$taskRunId",
      "TimeSeriesType": "$timeSeriesType",
      "Comment": "$timeseries.c",
      "DisplayValue": "$timeseries.dv",
      "Flag": "$timeseries.f",
      "LocalTime": "$timeseries.lt",
      "Time": "$timeseries.t",
      "Value": "$timeseries.v"
    }
  }
]);
db.getCollection("view.SimulatedHistoricalScalarTimeSeriesHeader").drop()
db.createView("view.SimulatedHistoricalScalarTimeSeriesHeader", "SimulatedHistoricalScalarTimeSeries", [
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "ForecastTime": "$forecastTime",
      "LocalEndTime": "$localEndTime",
      "LocalForecastTime": "$localForecastTime",
      "LocalStartTime": "$localStartTime",
      "LocationId": "$locationId",
      "MetaDataApprovedTime": "$metaData.approvedTime",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "RunInfoConfigRevisionNumber": "$runInfo.configRevisionNumber",
      "RunInfoDispatchTime": "$runInfo.dispatchTime",
      "RunInfoMcId": "$runInfo.mcId",
      "RunInfoTaskRunId": "$runInfo.taskRunId",
      "RunInfoTime0": "$runInfo.time0",
      "RunInfoUserId": "$runInfo.userId",
      "RunInfoWorkflowId": "$runInfo.workflowId",
      "StartTime": "$startTime",
      "TaskRunId": "$taskRunId",
      "TimeSeriesType": "$timeSeriesType"
    }
  }
]);
db.getCollection("view.SimulatedHistoricalScalarTimeSeriesStitched").drop()
db.createView("view.SimulatedHistoricalScalarTimeSeriesStitched", "SimulatedHistoricalScalarTimeSeriesStitched", [
  {
    "$unwind": "$timeseries"
  },
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Bucket": "$bucket",
      "BucketSize": "$bucketSize",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "LocationId": "$locationId",
      "MetaDataApprovedTime": "$metaData.approvedTime",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "StartTime": "$startTime",
      "TimeSeriesType": "$timeSeriesType",
      "Comment": "$timeseries.c",
      "DisplayValue": "$timeseries.dv",
      "Flag": "$timeseries.f",
      "LocalTime": "$timeseries.lt",
      "Time": "$timeseries.t",
      "Value": "$timeseries.v"
    }
  }
]);
db.getCollection("view.SimulatedHistoricalScalarTimeSeriesStitchedHeader").drop()
db.createView("view.SimulatedHistoricalScalarTimeSeriesStitchedHeader", "SimulatedHistoricalScalarTimeSeriesStitched", [
  {
    "$project": {
      "_id": 0,
      "ID": "$_id",
      "Bucket": "$bucket",
      "BucketSize": "$bucketSize",
      "Committed": "$committed",
      "EncodedTimeStepId": "$encodedTimeStepId",
      "EndTime": "$endTime",
      "EnsembleId": "$ensembleId",
      "EnsembleMemberId": "$ensembleMemberId",
      "LocationId": "$locationId",
      "MetaDataApprovedTime": "$metaData.approvedTime",
      "MetaDataArchiveTime": "$metaData.archiveTime",
      "MetaDataAreaId": "$metaData.areaId",
      "MetaDataDisplayUnit": "$metaData.displayUnit",
      "MetaDataEnsembleMemberIndex": "$metaData.ensembleMemberIndex",
      "MetaDataLocalTimeZone": "$metaData.localTimeZone",
      "MetaDataLocationName": "$metaData.locationName",
      "MetaDataModifiedTime": "$metaData.modifiedTime",
      "MetaDataParameterName": "$metaData.parameterName",
      "MetaDataParameterType": "$metaData.parameterType",
      "MetaDataSourceId": "$metaData.sourceId",
      "MetaDataTimeStepLabel": "$metaData.timeStepLabel",
      "MetaDataTimeStepMinutes": "$metaData.timeStepMinutes",
      "MetaDataUnit": "$metaData.unit",
      "ModuleInstanceId": "$moduleInstanceId",
      "ParameterId": "$parameterId",
      "QualifierId": "$qualifierId",
      "StartTime": "$startTime",
      "TimeSeriesType": "$timeSeriesType"
    }
  }
]);
