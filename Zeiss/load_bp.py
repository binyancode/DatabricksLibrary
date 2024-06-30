from DatabricksHelper.Service import TableLoadingStreamingProcessor
#error: validation_result < 0
class BPLoadingStreamingProcessor(TableLoadingStreamingProcessor):
    def validate(self, row):
        result = []
        if row.Data is None or row.Data == "":
            result.append({"name":"DataIsNull","result":-1, "message":"Data is invalid."})
        else:
            result.append({"name":"DataIsNull","result":0, "message":"Data is valid."})

        if row.EvaIbaseCorrelationId is None or row.EvaIbaseCorrelationId == "string":
            result.append({"name":"EvaIbaseCorrelationIdIsInvalid","result":-1, "message":"EvaIbaseCorrelationId is invalid."})
        else:
            result.append({"name":"EvaIbaseCorrelationIdIsInvalid","result":0, "message":"EvaIbaseCorrelationId is valid."})
        return result
    
    def with_columns(self):
        return []
    
    def process_cell(self, row, column_name, validations):
        return row[column_name]