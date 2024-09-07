CREATE PROCEDURE SalesNipun.MergeMetadata
    @DatasetId INT,
    @LocalRunDatetime NVARCHAR(MAX)
AS
BEGIN
    -- Declare variables to hold the output values
    DECLARE @LoadType VARCHAR(1)
    DECLARE @RawLastFullLoadDate DATETIME2
    DECLARE @RawLastIncrementalLoadDate DATETIME2
    DECLARE @RawLastPipelineRunId NVARCHAR(50)
    DECLARE @DatasetName NVARCHAR(50)
    DECLARE @SchemaName NVARCHAR(50)
    DECLARE @PrimaryKeyFields NVARCHAR(50)
    DECLARE @StagingType NVARCHAR(20)
    DECLARE @UpdateType NVARCHAR(20)
    DECLARE @Year INT
    DECLARE @Month INT
    DECLARE @Day INT
    
    -- If @LocalRunDatetime is NULL or empty, set it to the current date and time
    SET @LocalRunDatetime = LTRIM(RTRIM(@LocalRunDatetime));
    IF @LocalRunDatetime = ''
    BEGIN
        SET @LocalRunDatetime = CONVERT(DATETIME, SWITCHOFFSET(GETDATE(), '+07:00'));
    END
 
    -- Check StagingType of the DatasetId
    SELECT
        @StagingType = StagingType,
        @RawLastFullLoadDate = RawLastFullLoadDate,
        @RawLastIncrementalLoadDate = RawLastIncrementalLoadDate,
        @RawLastPipelineRunId = RawLastPipelineRunId,
        @DatasetName = DatasetName,
        @LoadType = LoadType,
        @UpdateType = UpdateType,
        @SchemaName = SchemaName,
        @PrimaryKeyFields = PrimaryKeyFields

    FROM 
        SalesNipun.DatasetsMetadata
    WHERE
        DatasetId = @DatasetId
    
    -- Set the Year, Month, and Day variables based on the load dates
    IF @RawLastIncrementalLoadDate IS NULL
    BEGIN
        SET @Year = YEAR(@RawLastFullLoadDate);
        SET @Month = MONTH(@RawLastFullLoadDate);  
        SET @Day = DAY(@RawLastFullLoadDate);
    END
    ELSE 
    BEGIN
        SET @Year = YEAR(@RawLastIncrementalLoadDate);
        SET @Month = MONTH(@RawLastIncrementalLoadDate);  
        SET @Day = DAY(@RawLastIncrementalLoadDate);
    END

    IF @StagingType = 'parquet'
    BEGIN
        -- Set @Path variable
        DECLARE @Path NVARCHAR(MAX);
        SET @Path = '/raw/sql_server/' + @DatasetName +
                    '/year=' + CAST(@Year AS NVARCHAR(4)) +
                    '/month=' + CAST(@Month AS NVARCHAR(2)) +
                    '/day=' + CAST(@Day AS NVARCHAR(2)) +
                    '/run_id=';

        -- Output the result
        SELECT @Path AS Path, @DatasetName AS DatasetName
            , @SchemaName AS SchemaName, @LoadType AS LoadType, @UpdateType AS UpdateType, @PrimaryKeyFields AS PrimaryKeyFields
            , @StagingType  AS StagingType, @RawLastPipelineRunId AS RawLastPipelineRunId
           	, @LocalRunDatetime AS LocalRunDatetime;
    END

    ELSE IF @StagingType = 'csv'
    BEGIN 
        DECLARE @CsvPath NVARCHAR(100)
        DECLARE @CsvDelimiter NVARCHAR(50)

        SELECT
            @DatasetName = DatasetName,
            @CsvPath = CsvPath,
            @CsvDelimiter = CsvDelimiter,
            @LoadType = LoadType,
            @UpdateType = UpdateType,
            @RawLastPipelineRunId = RawLastPipelineRunId,
            @PrimaryKeyFields = PrimaryKeyFields,
            @SchemaName = SchemaName

        FROM 
            SalesNipun.DatasetsMetadata
        WHERE
            DatasetId = @DatasetId
    END
    
    SELECT  @LocalRunDatetime AS LocalRunDatetime, @DatasetName AS DatasetName
           , @LoadType AS LoadType, @UpdateType AS UpdateType, @PrimaryKeyFields AS PrimaryKeyFields
           , @CsvPath AS CsvPath, @CsvDelimiter AS CsvDelimiter, @StagingType  AS StagingType
           , @RawLastPipelineRunId AS RawLastPipelineRunId, @SchemaName AS SchemaName
END
