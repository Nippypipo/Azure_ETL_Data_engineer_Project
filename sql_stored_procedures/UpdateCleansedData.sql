
CREATE PROCEDURE SalesNipun.UpdateCleansedData
    @DatasetId INT,
    @PipelineRunId uniqueidentifier,
    @LocalRunDatetime DATETIME2

AS
BEGIN
    DECLARE @CleansedLastFullLoadDate DATETIME2;
    DECLARE @LoadType NVARCHAR(1);
    -- query 2 LoadDate column
    SELECT
        @CleansedLastFullLoadDate = CleansedLastFullLoadDate,
        @LoadType = LoadType
    FROM
        SalesNipun.DatasetsMetadata

    WHERE
        @DatasetId = DatasetId
    
    -- set LoadDate value
    IF @CleansedLastFullLoadDate IS NULL OR @LoadType = 'F'
    BEGIN
        UPDATE SalesNipun.DatasetsMetadata
        SET 
            CleansedLastFullLoadDate = @LocalRunDatetime,
            RawLastPipelineRunId = @PipelineRunId
        WHERE DatasetId = @DatasetId
    END

    ELSE
    BEGIN
        UPDATE SalesNipun.DatasetsMetadata
        SET 
            CleansedLastIncrementalLoadDate = @LocalRunDatetime,
            RawLastPipelineRunId = @PipelineRunId
        WHERE DatasetId = @DatasetId
    END
END


