# azure_etl_data_engineer
---

ETL Pipeline แรกกับการใช้ Microsoft Azure | End-to-end Data Engineer Project

สวัสดีครับ วันนี้ผมจะมาลองทำโปรเจกต์ ETL pipeline ที่ผมได้เรียนรู้จากการฝึกงาน Data Engineer ที่ BBIK ครับ
Overview
ภาพรวมโปรเจกต์นี้จะทำ ETL pipeline ตั้งแต่ดึงข้อมูลจาก Data source ไปจนถึงส่งข้อมูลที่มีคุณภาพแล้วมานำเสนอเป็น Dashboard ที่ PowerBI โดยใช้ Microsoft Azure: Cloud Computing Services เป็นหลัก 
สำหรับ ETL process ในโปรเจกต์นี้ก็คือ Extract ข้อมูลจาก Data source (SQL server) ตามด้วย Transformation ข้อมูลที่ Staging (Data lake) ก่อนที่จะ Load เข้า Lakehouse (Delta Lake)
แผนภาพโปรเจกต์โดยรวมData Engineer Lifecycle ในโปรเจกต์นี้จะครอบคลุมตั้งแต่ 
Ingestion: ดึงข้อมูลจาก Data source ต่าง ๆ เช่น SQL server เพื่อนำข้อมูลจาก Database ไปเก็บไว้ที่ Staging และทำการ Merge ข้อมูลเข้าที่ Lakehouse 
Transformation: ทำการแปลงข้อมูลใน Lakehouse ให้อยู่ในรูปแบบที่พร้อมสำหรับการนำไปวิเคราะห์
Serving: เชื่อมข้อมูลจาก Lakehouse ไปที่ BI Tool อย่าง PowerBI เพื่อทำ Dashboard ตาม Business requirements

Topics covered
Techstack 
Project setup 
ETL Pipeline: Ingestion, Transformation, Serving
Unit testing
Conclusion 

---

Techstack
เครื่องมือที่ใช้ในโปรเจกต์ จะมีดังนี้  
Microsoft Azure
Azure Data Factory: ใช้สำหรับเป็น Orchestration tool ในการสั่งการควบคุม Data Pipeline
Azure Databricks: ใช้สำหรับเป็น Notebook ในการรัน Spark Cluster เพื่อใช้ในการ Merge และ Transform ข้อมูล
Azure Synapse Analytics: ใช้สำหรับเป็น Notebook ในการรัน Spark Cluster เพื่อใช้ในการ Merge และ Transform ข้อมูล (อีกทางเลือกหนึ่งหากไม่ใช้ Databricks)
Azure Data Lake Storage: ใช้สำหรับเป็นที่พื้นเก็บข้อมูล Landing Zone
Azure Storage Explorer: ใช้สำหรับจัดการ Storage ให้ง่ายขึ้น ทั้ง Data lake และ Lakehouse

Coding
Python: จัดการ Notebook ในการ Merge และ Transform ข้อมูล โดยใช้ Pyspark library เพื่อใช้ Spark
SQL: จัดการข้อมูล Database และสร้าง Stored Procedure

Management Tools
DBeaver: จัดการ Database MySQL Server
Github: ติดตามการเปลี่ยนแปลงของโปรแกรมและจัดการไฟล์ในโปรเจกต์

Project Setup
Metadata Table
Dataset Metadata table ด้านซ้าย และ Dataset Metadata Sample ด้านขวาเป็นตารางใน Database ที่รวบรวม Metadata ของ Dataset ที่ต้องการโหลดเข้า Lakehouse 
ส่วนที่ 1: ข้อมูลทั่วไปของ Dataset ที่ต้องการ ในกรณีนี้ใช้สำหรับสร้าง Query script ในการดึงข้อมูล
DatasetId
DatasetName
SchemaName
TableName
SelectedFields
WhereClause

ส่วนที่ 2: ข้อมูลวิธีการโหลดและอัปเดต Dataset
LoadType 

F (Full Load) โหลดข้อมูลทั้งหมด
I (Incremental) โหลดข้อมูลเพิ่มบางส่วนโดยใช้ Condition ในการ Query จาก IncrementalFields
UpdateType 

scd2 (Slowly change dimension Type 2) จะทำการอัปเดตตารางโดยเพิ่มแถวและคอลัมน์เพื่อเก็บประวัติข้อมูล ได้แก่คอลัมน์ Effective Start Date และ Effective End Date เพื่อช่วยในการตรวจสอบว่าแถวข้อมูลใดที่เลิกใช้งานแล้ว และสร้างคอลัมน์ Flag ข้อมูลที่ใช้งานได้อยู่ เมื่อลักษณะข้อมูลเปลี่ยนแปลง ข้อมูลเดิมจะถูกเก็บไว้ และข้อมูลใหม่จะถูกเพิ่มในแถวใหม่ เพื่อให้เราสามารถตรวจสอบประวัติการเปลี่ยนแปลงของข้อมูลได้
op (Overwrite Partition) จะทำการอัปเดตตารางโดยโหลดใหม่เฉพาะบาง Partition เท่านั้น จึงต้องกำหนด Condition ในการอัปเดตจาก PartitionFields ไว้ เพื่อให้เราไม่ต้องโหลดข้อมูลใหม่ทั้งหมด
PartitionFields
PrimaryKeyFields
IncrementalFields

ส่วนที่ 3: ข้อมูลการโหลดของ Dataset จาก Datasource สู่ Staging
RawLastPipelineRunId
RawLastFullLoadDate
RawLastIncrementalLoadDate

ส่วนที่ 4: ข้อมูลการอัปเดตของ Dataset จาก Staging สู่ Lakehouse
CleansedSchemaName
CleansedLastFullLoadDate
CleansedLastIncrementalLoadDate

Ingestion
Ingestion pipeline

โหลดข้อมูลจาก SQL server มาไว้ที่ Azure Data Lake Storage Gen 2 (Staging) ในรูปแบบ Parquet ไฟล์ ซึ่งจะใช้ Azure Data Factory ในการควบคุม Pipeline
ภาพ Ingestion pipeline ใน ADFกระบวนการในการทำงานใน Ingestion Pipeline จะเป็นดังนี้
ใช้กล่อง Lookup ในการเรียก Stored Procedure1 (PayloadGenerator.sql) เพื่อดึงข้อมูล Dataset Metadata Table มาสร้าง Query script สำหรับ Dataset ที่เลือก 
ใช้กล่อง Copy data ในการนำ Query script มาดึงข้อมูล Dataset และนำไปเก็บที่ ADS2 ตาม Path template ที่กำหนดไว้
ใช้กล่อง Stored procedure ในการสั่ง Stored Procedure2 (UpdateMetadata.sql) เพื่ออัปเดตข้อมูลการโหลดลง Dataset Metadata Table

Requirements การอัปเดตข้อมูลบน Stored Procedure จะต้องทำได้ทั้ง Full Load และ Incremental Load … ไว้มาต่อ
2. Merging pipeline 
สร้าง Spark Notebook เพื่ออ่านข้อมูล Parquet ไฟล์ที่เก็บไว้ที่ Azure Data Lake Storage Gen 2 (Staging) 
ภาพ Merging pipeline ใน ADFกระบวนการในการทำงานใน Merging Pipeline จะเป็นดังนี้
ใช้กล่อง Lookup ในการเรียก Stored Procedure1 (PayloadGenerator.sql) เพื่อดึงข้อมูล Dataset Metadata Table มาสร้าง Query script สำหรับ Dataset ที่เลือก
ใช้กล่อง Copy data ในการนำ Query script มาดึงข้อมูล Dataset และนำไปเก็บที่ ADS2 ตาม Path template ที่กำหนดไว้
ใช้กล่อง Stored procedure ในการสั่ง Stored Procedure2 (UpdateMetadata.sql) เพื่ออัปเดตข้อมูลการโหลดลง Dataset Metadata Table

ใน Merging Pipeline จะมีรายละเอียดเรื่องการสร้างและอัปเดตข้อมูลใน Delta lake โดย Merge Notebook จะมีกระบวนการดังแผนภาพนี้
กระบวนการใน Notebook ตัวอย่าง Merge function Notebook ใน ADFTransformation

Serving

Unit testing

Conclusion

github: 
Thayawat Wangsamphao 

References
slowly change dimension type2 function
concept of scd2
unit test in databricks

Appendix
ตัวอย่างโค้ด Stored Procedure 1: PayloadGenerator
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
