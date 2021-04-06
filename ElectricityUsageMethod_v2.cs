// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Azure.Solutions.SmartUtility.Core.Contracts
{
    public enum ElectricityUsageMethod
    {
        Consumption = 1,
        Generation = 2,
        ConsumptionIntoStorage = 3,
        OptimizedConsumptionIncrease = 4,
        OptimizedConsumptionDecrease = 5,
        GenerationFromStorage = 6,
    }
}
