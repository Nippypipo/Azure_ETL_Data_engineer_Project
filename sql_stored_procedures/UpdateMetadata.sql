
CREATE PROCEDURE SalesNipun.UpdateMetadata
    @DatasetId INT,
    @PipelineRunId uniqueidentifier,
    @LocalRunDatetime DATETIME2

AS
BEGIN
    DECLARE @RawLastFullLoadDate DATETIME2;
    DECLARE @LoadType NVARCHAR(1);
    -- query 2 LoadDate column
    SELECT
        @RawLastFullLoadDate = RawLastFullLoadDate,
        @LoadType = LoadType
    FROM
        SalesNipun.DatasetsMetadata

    WHERE
        @DatasetId = DatasetId
    
    -- set LoadDate value
    IF @RawLastFullLoadDate IS NULL OR @LoadType = 'F'
    BEGIN
        UPDATE SalesNipun.DatasetsMetadata
        SET 
            RawLastFullLoadDate = @LocalRunDatetime,
            RawLastPipelineRunId = @PipelineRunId
        WHERE DatasetId = @DatasetId
    END

    ELSE
    BEGIN
        UPDATE SalesNipun.DatasetsMetadata
        SET 
            RawLastIncrementalLoadDate = @LocalRunDatetime,
            RawLastPipelineRunId = @PipelineRunId
        WHERE DatasetId = @DatasetId
    END
END


