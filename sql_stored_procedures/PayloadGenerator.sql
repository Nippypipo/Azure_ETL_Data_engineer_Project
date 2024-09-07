CREATE PROCEDURE SalesNipun.PayloadGenerator
    @DatasetId INT,
    @LocalRunDatetime NVARCHAR(MAX)
AS
BEGIN
    -- Declare variables to hold the output values
    DECLARE @LoadType VARCHAR(1)
    DECLARE @RawLastFullLoadDate DATETIME2
    DECLARE @RawLastIncrementalLoadDate DATETIME2
    DECLARE @RawLastPipelineRunId NVARCHAR(50)
    DECLARE @IncrementalField1 NVARCHAR(50)
    DECLARE @IncrementalField2 NVARCHAR(50)
    DECLARE @DatasetName NVARCHAR(50)
    DECLARE @SchemaName NVARCHAR(50)
    DECLARE @PrimaryKeyFields NVARCHAR(50)
    DECLARE @StagingType NVARCHAR(20)
    DECLARE @UpdateType NVARCHAR(20)

    -- If @LocalRunDatetime is NULL or empty, set it to the current date and time
    SET @LocalRunDatetime = LTRIM(RTRIM(@LocalRunDatetime));
    IF @LocalRunDatetime = ''
    BEGIN
        SET @LocalRunDatetime = CONVERT(DATETIME, SWITCHOFFSET(GETDATE(), '+07:00'));
    END
    
    DECLARE @Year INT = YEAR(@LocalRunDatetime);
    DECLARE @Month INT = MONTH(@LocalRunDatetime);  
    DECLARE @Day INT = DAY(@LocalRunDatetime);

    -- Check StagingType of the DatasetId
    SELECT
        @StagingType = StagingType
    FROM 
        SalesNipun.DatasetsMetadata
    WHERE
        DatasetId = @DatasetId
    
    IF @StagingType = 'parquet'
    BEGIN
        DECLARE @Query NVARCHAR(MAX)
        SELECT
        @Query = 'SELECT ' + SelectedFields + ' FROM ' + SchemaName + '.' + TableName + 
                    CASE 
                        WHEN WhereClause IS NOT NULL AND LTRIM(RTRIM(WhereClause)) <> '' THEN ' WHERE ' + WhereClause 
                        ELSE '' 
                    END,
            @LoadType = LoadType,
            @RawLastFullLoadDate = RawLastFullLoadDate,
            @RawLastIncrementalLoadDate = RawLastIncrementalLoadDate,
            @RawLastPipelineRunId = RawLastPipelineRunId,
            @IncrementalField1 = IncrementalField1,
            @IncrementalField2 = IncrementalField2,
            @DatasetName = DatasetName,
            @SchemaName = SchemaName,
            @PrimaryKeyFields = PrimaryKeyFields,
            @UpdateType = UpdateType
        FROM 
            SalesNipun.DatasetsMetadata
        WHERE
            DatasetId = @DatasetId

        IF @LoadType = 'F'
        BEGIN
            SET @Query = @Query
        END
        ELSE IF @LoadType = 'I'
        BEGIN
            IF @RawLastFullLoadDate IS NULL OR LTRIM(RTRIM(@RawLastFullLoadDate)) <> ''
            BEGIN
                SET @Query = @Query
            END

            ELSE
            BEGIN
                IF @Query LIKE '%WHERE%'
                BEGIN
                    SET @Query = @Query + ' AND ' + @IncrementalField1 + ' >= ''' + CAST(DATEADD(DAY, -1, @LocalRunDatetime) AS NVARCHAR(50)) + ''''
                    SET @Query = @Query + ' AND ' + @IncrementalField1 + ' < ''' + CAST(@LocalRunDatetime AS NVARCHAR(50)) + ''''

                    IF @IncrementalField2 IS NOT NULL AND LTRIM(RTRIM(@IncrementalField2)) <> ''
                    BEGIN
                        SET @Query = @Query + ' AND ' + @IncrementalField2 + ' >= ''' + CAST(DATEADD(DAY, -1, @LocalRunDatetime) AS NVARCHAR(50)) + ''''
                        SET @Query = @Query + ' AND ' + @IncrementalField2 + ' < ''' + CAST(@LocalRunDatetime AS NVARCHAR(50)) + ''''
                    END
                END
                ELSE
                BEGIN
                    SET @Query = @Query + ' WHERE ' + @IncrementalField1 + ' >= ''' + CAST(DATEADD(DAY, -1, @LocalRunDatetime) AS NVARCHAR(50)) + ''''
                    SET @Query = @Query + ' AND ' + @IncrementalField1 + ' < ''' + CAST(@LocalRunDatetime AS NVARCHAR(50)) + ''''

                    IF @IncrementalField2 IS NOT NULL AND LTRIM(RTRIM(@IncrementalField2)) <> ''
                    BEGIN
                        SET @Query = @Query + ' AND ' + @IncrementalField2 + ' >= ''' + CAST(DATEADD(DAY, -1, @LocalRunDatetime) AS NVARCHAR(50)) + ''''
                        SET @Query = @Query + ' AND ' + @IncrementalField2 + ' < ''' + CAST(@LocalRunDatetime AS NVARCHAR(50)) + ''''
                    END
                END
            END
        END
        
        -- Set @Path variable
        DECLARE @Path NVARCHAR(MAX);
        SET @Path = '/raw/sql_server/' + @DatasetName +
                    '/year=' + CAST(@Year AS NVARCHAR(4)) +
                    '/month=' + CAST(@Month AS NVARCHAR(2)) +
                    '/day=' + CAST(@Day AS NVARCHAR(2)) +
                    '/run_id=';

        -- Output the result
        SELECT @Query AS Query, @Path AS Path, @LocalRunDatetime AS LocalRunDatetime, @DatasetName AS DatasetName
            , @SchemaName AS SchemaName, @LoadType AS LoadType, @UpdateType AS UpdateType, @PrimaryKeyFields AS PrimaryKeyFields
            , @StagingType  AS StagingType, @RawLastPipelineRunId AS RawLastPipelineRunId;
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
            @UpdateType = UpdateType


        FROM 
            SalesNipun.DatasetsMetadata
        WHERE
            DatasetId = @DatasetId

    END
    SELECT  @LocalRunDatetime AS LocalRunDatetime, @DatasetName AS DatasetName
           , @LoadType AS LoadType, @UpdateType AS UpdateType, @PrimaryKeyFields AS PrimaryKeyFields
           , @CsvPath AS CsvPath, @CsvDelimiter AS CsvDelimiter, @StagingType  AS StagingType
END
