CREATE PROCEDURE [etl].[usp_TransformEnergyMeasurements]
AS
BEGIN
    DECLARE @tableName NVARCHAR(256) = 'usp_TransformEnergyMeasurements'
    DECLARE @fromWatermark INT
    DECLARE @toWatermark INT
    DECLARE @batchSize INT = 1000
    DECLARE @minStartDateTime DATETIME2
    DECLARE @now DATETIME2 = GETUTCDATE()

    SELECT  @fromWatermark = [watermark]
    FROM    etl.tbl_ChangeTracking
    WHERE   tableName = @tableName

    IF @fromWatermark IS NULL
    BEGIN
        SET @fromWatermark = 0
        INSERT etl.tbl_ChangeTracking ([tableName] ,[watermark])
        VALUES (@tableName, @fromWatermark)
    END

    DROP TABLE IF EXISTS #Devices
    CREATE TABLE #Devices
    (
        [deviceId]              NVARCHAR(200)   PRIMARY KEY,
        [DeviceSK]              INT             NOT NULL
    )

    DROP TABLE IF EXISTS #Batch
    CREATE TABLE #Batch (
        [id]                 INT            PRIMARY KEY,
        [timestamp]          DATETIME2      NOT NULL,
        [timestampPred]      DATETIME2      NOT NULL,
        [timestampSucc]      DATETIME2      NULL,
        [deviceId]           NVARCHAR(200)  NOT NULL,
        [properties]         NVARCHAR(MAX)  NOT NULL,
        [systemProperties]   NVARCHAR(MAX)  NOT NULL,
        [body]               NVARCHAR(MAX)  NOT NULL,
        [DeviceSK]           INT            NOT NULL
    )

    DROP TABLE IF EXISTS #Measurements
    CREATE TABLE #Measurements (
        [id]                                INT                 PRIMARY KEY,
        [DeviceSK]                          INT                 NOT NULL,
        [StartDateTime]                     DATETIME2           NOT NULL,
        [EndDateTime]                       DATETIME2           NOT NULL,
        [ConsumptionEnergy_kWh]             FLOAT               NOT NULL,
        [GenerationEnergy_kWh]              FLOAT               NOT NULL,
        [ConsumptionIntoStorageEnergy_kWh]  FLOAT               NOT NULL
    )

    DROP TABLE IF EXISTS #Intervals
    CREATE TABLE #Intervals
    (
        DateSK          INT         NOT NULL,
        TimeSK          INT         NOT NULL,
        StartDateTime   DATETIME2   NOT NULL PRIMARY KEY,
        EndDateTime     DATETIME2   NOT NULL,
    )

    DROP TABLE IF EXISTS #Source
    CREATE TABLE #Source
    (
        [DateSK]                            INT                 NOT NULL,
        [TimeSK]                            INT                 NOT NULL,
        [DeviceSK]                          INT                 NOT NULL,
        [StartDateTime]                     DATETIME2           NOT NULL,
        [EndDateTime]                       DATETIME2           NOT NULL,
        [MeasurementsCount]                 INT                 NOT NULL,
        [ConsumptionEnergy_kWh]             DECIMAL(38,12)      NOT NULL,
        [GenerationEnergy_kWh]              DECIMAL(38,12)      NOT NULL,
        [ConsumptionIntoStorageEnergy_kWh]  DECIMAL(38,12)      NOT NULL,
        [Duration_ms]                       INT                 NOT NULL,
    )

    -- Save deviceId for Distributed Energy Resoruces together with their keys.
    INSERT  #Devices
    SELECT  DeviceId,
            [DistributedEnergyResourceSK]
    FROM    analytics.DistributedEnergyResources
    WHERE   DeviceTemplateName = 'EnergyResource'

    --Save a batch of messages to process.
    ;WITH Batch AS 
    (
        SELECT  TOP(@batchSize) 
                m.*,
                d.DeviceSK
        FROM    stage.tbl_MessagesPast AS m
        JOIN    #Devices AS d 
        ON      d.deviceId = m.deviceId
        WHERE   id > @fromWatermark
        ORDER BY id ASC
        UNION ALL
        SELECT  TOP(@batchSize) 
                m.*,
                d.DeviceSK
        FROM    stage.tbl_MessagesLast AS m
        JOIN    #Devices AS d 
        ON      d.deviceId = m.deviceId
        WHERE   id > @fromWatermark
        ORDER BY id ASC
    )
    INSERT INTO #Batch
    SELECT  TOP(@batchSize) *
    FROM    Batch 
    ORDER BY id ASC

    INSERT INTO #Measurements
    SELECT	m.id,
            m.DeviceSK,
            b.StartDateTime,
            b.EndDateTime,
            ISNULL(CASE WHEN b.UsageMethod = 1 THEN b.Energy_kWh END, 0.0) AS ConsumptionEnergy_kWh,
            ISNULL(CASE WHEN b.UsageMethod = 2 THEN b.Energy_kWh END, 0.0) AS GenerationEnergy_kWh,
            ISNULL(CASE WHEN b.UsageMethod = 3 THEN b.Energy_kWh END, 0.0) AS ConsumptionIntoStorageEnergy_kWh,
            ISNULL(CASE WHEN b.UsageMethod = 6 THEN b.Energy_kWh END, 0.0) AS DischargeFromStorageEnergy_kWh
    FROM #Batch as m
    OUTER APPLY OPENJSON(m.body) WITH
    (
        StartDateTime DATETIME2 N'$.StartDateTime',
        EndDateTime DATETIME2 N'$.EndDateTime',
        Energy_kWh FLOAT N'$.EnergyAmountkWh',
        UsageMethod INT N'$.UsageMethod'
    ) AS b
    WHERE b.StartDateTime IS NOT NULL
        AND b.EndDateTime IS NOT NULL
        AND b.Energy_kWh IS NOT NULL

    -- Save watermark processing of the batch will reach.
    SELECT  @toWatermark = MAX(id),
            @minStartDateTime = MIN(StartDateTime)
    FROM    #Measurements

    -- Prepare table of time intervals relevant to the batch.
    INSERT INTO #Intervals
    SELECT  d.DateSK,
            t.TimeSK,
            DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t.StartTime), d.Date) AS StartDateTime,
            DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t.EndTime), d.Date) AS EndDateTime
    FROM    analytics.Date as d
    CROSS JOIN analytics.Time AS t
    WHERE   d.Date BETWEEN DATEADD(DAY, -1, @minStartDateTime)  AND DATEADD(DAY, 1, @now)

    -- Prepare source table. 
    ;WITH [FractionalMeasurements] AS 
    (
        SELECT  i.DateSK,
                i.TimeSK,
                i.StartDateTime,
                i.EndDateTime,
                m.id,
                m.DeviceSK,
                CONVERT(FLOAT, 
                    DATEDIFF(MILLISECOND,
                        (CASE WHEN m.StartDateTime > i.StartDateTime THEN m.StartDateTime ELSE i.StartDateTime END),
                        (CASE WHEN m.EndDateTime < i.EndDateTime THEN m.EndDateTime ELSE i.EndDateTime END)
                )) / CONVERT(FLOAT, DATEDIFF(MILLISECOND, i.StartDateTime, i.EndDateTime))
                AS ContributionFactor,
                m.ConsumptionEnergy_kWh,
                m.GenerationEnergy_kWh,
                m.ConsumptionIntoStorageEnergy_kWh,
                CONVERT(FLOAT, DATEDIFF(MILLISECOND,
                    (CASE WHEN m.StartDateTime > i.StartDateTime THEN m.StartDateTime ELSE i.StartDateTime END),
                    (CASE WHEN m.EndDateTime < i.EndDateTime THEN m.EndDateTime ELSE i.EndDateTime END)
                )) AS Duration_ms
        FROM    #Measurements m
        CROSS JOIN #Intervals AS i
        WHERE   m.StartDateTime < i.EndDateTime
                AND m.EndDateTime > i.StartDateTime
    ),
    [Contributions] AS
    (
        SELECT	id,
                SUM(ContributionFactor) AS ContributionTotal
        FROM FractionalMeasurements
        GROUP BY id
    ),
    [Source] AS 
    (
        SELECT	m.DateSK,
                m.TimeSK,
                m.DeviceSK,
                MIN(m.StartDateTime) AS StartDateTime,
                MIN(m.EndDateTime) AS EndDateTime,
                COUNT(*) AS MeasurementsCount,
                SUM((m.ConsumptionEnergy_kWh * m.ContributionFactor) / c.ContributionTotal) AS ConsumptionEnergy_kWh,
                SUM((m.GenerationEnergy_kWh * m.ContributionFactor) / c.ContributionTotal) AS GenerationEnergy_kWh,
                SUM((m.ConsumptionIntoStorageEnergy_kWh * m.ContributionFactor) / c.ContributionTotal) AS ConsumptionIntoStorageEnergy_kWh,
                SUM(m.Duration_ms) AS Duration_ms
        FROM    [FractionalMeasurements] AS m
                INNER JOIN Contributions AS c
                ON m.id = c.id
        GROUP BY m.DateSK, m.TimeSK, m.DeviceSK
    )
    INSERT  #Source
    SELECT *
    FROM [Source]

    -- Update target table and change tracking table in a single transaction.
    BEGIN TRANSACTION
    BEGIN TRY 
        MERGE   [analytics].[Measurements] AS t
        USING   #Source AS s
        ON      t.DateSK = s.DateSK
                AND t.TimeSK = s.TimeSK
                AND t.DeviceSK = s.DeviceSK
        WHEN MATCHED THEN
        UPDATE SET
                t.[MeasurementsCount]                   = ISNULL(t.[MeasurementsCount], 0) + s.[MeasurementsCount],
                t.[ConsumptionEnergy_kWh]               = ISNULL(t.[ConsumptionEnergy_kWh], 0.0) + s.[ConsumptionEnergy_kWh],
                t.[GenerationEnergy_kWh]                = ISNULL(t.[GenerationEnergy_kWh], 0.0) + s.[GenerationEnergy_kWh],
                t.[ConsumptionIntoStorageEnergy_kWh]    = ISNULL(t.[ConsumptionIntoStorageEnergy_kWh], 0.0) + s.[ConsumptionIntoStorageEnergy_kWh],
                t.[Duration_ms]                         = ISNULL(t.[Duration_ms], 0) + s.[Duration_ms]
        WHEN NOT MATCHED BY TARGET 
        THEN
        INSERT  (
                [DateSK],
                [TimeSK],
                [DeviceSK],
                [StartDateTime],
                [EndDateTime],
                [MeasurementsCount],
                [ConsumptionEnergy_kWh],
                [GenerationEnergy_kWh],
                [ConsumptionIntoStorageEnergy_kWh],
                [Duration_ms]
        ) VALUES (
                s.[DateSK],
                s.[TimeSK],
                s.[DeviceSK],
                s.[StartDateTime],
                s.[EndDateTime],
                s.[MeasurementsCount],
                s.[ConsumptionEnergy_kWh],
                s.[GenerationEnergy_kWh],
                s.[ConsumptionIntoStorageEnergy_kWh],
                s.[Duration_ms]
        );

        UPDATE  etl.tbl_ChangeTracking
        SET     [watermark] = @toWatermark
        WHERE   tableName = @tableName
                AND [watermark] <> @toWatermark

        COMMIT TRANSACTION
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION
        THROW
    END CATCH

    -- Clean up temporary tables.
    DROP TABLE #Devices
    DROP TABLE #Batch
    DROP TABLE #Measurements
    DROP TABLE #Intervals
    DROP TABLE #Source
END