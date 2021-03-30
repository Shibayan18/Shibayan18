CREATE TABLE [optimization].[BatteryInstructions]
 (
    [DistributedEnergyResourceSK] INT            IDENTITY (1, 1) NOT NULL,
    [DeviceId]                    NVARCHAR (200) NOT NULL,
    [Name]                        NVARCHAR (200) NOT NULL,
    [Simulated]                   BIT            NOT NULL,
    [MadeatDateTime]         DATETIME2      NOT NULL,
    [StartDateTime]         DATETIME2      NOT NULL,
    [EndDateTime]         DATETIME2      NOT NULL,
    [GridCarbonEmissionSK]          INT             IDENTITY (1, 1) NOT NULL,
    [MarginalCO2Intensity_Forcast_gCO2kWh] DECIMAL(10, 6) NULL,
    [Instruction_Issued]  INT NOT NULL,
    PRIMARY KEY CLUSTERED ([DistributedEnergyResourceSK] ASC)
);
GO


CREATE SCHEMA [optimization] 