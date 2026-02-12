# Data Processing Filters Documentation

Last updated: 2025-11-07

## Overview

This document provides a comprehensive list of all filters, thresholds, and data quality controls applied during the MTA ridership data processing pipeline. Understanding these filters is crucial for interpreting the final results and ensuring data quality.

## Filter Categories

Filters are applied at different stages of the pipeline to ensure data quality and consistency:

1. **Station & Division Filters** - Remove non-subway or problematic stations
2. **Time Range Filters** - Focus on specific date ranges
3. **Data Quality Filters** - Remove outliers and unrealistic values
4. **Geographic Filters** - Ensure proper station mapping
5. **Format Filters** - Handle data consistency issues

## Turnstile Data Filters

### Stage: Turnstile Staging (`scripts/local/stage_turnstile_data.py`)

#### Station Exclusions
- **Excluded Stations**: `["ORCHARD BEACH"]`
- **Rationale**: Not a real subway station - these were bus fare turnstiles at Orchard Beach (see [turnstile documentation](turnstile_data.md) for full story)

#### Division Filter
- **Allowed Divisions**: `["BMT", "IND", "IRT"]`
- **Excluded**: PTH (PATH), SRT (Staten Island Railway), RIT, JFK
- **Rationale**: Focus exclusively on NYC subway system, excluding other transit modes

#### Date Range Filter
- **Start Date**: October 18, 2014 (`MODERN_FORMAT_START_DATE = 141018`)
- **Rationale**: Only processes files in modern CSV format for consistency

#### Note on Complex ID Filters
- No Complex ID–based exclusions are applied during turnstile staging. Complex-level filters (e.g., Complex ID 502) are handled in modern ridership processing (see below).

### Stage: Turnstile Processing (`scripts/local/process_turnstile_data.py`)

#### Outlier Detection
- **Method**: Modal record count analysis
- **Threshold**: Remove turnstiles with < 10% of modal record count
- **Formula**: `record_count < modal_count * 0.1`
- **Rationale**: Identifies broken or intermittently reporting turnstiles

#### Time Interval Filter
- **Keep Only**: Records at `:00:00`, `:30:00`, or `:22:00`
- **Example**: Keep 08:00:00, 08:30:00, 08:22:00; Drop 08:15:00, 08:45:00
- **Special Case**: Complex ID 604 (161 St-Yankee Stadium) reports at :22:00 instead of standard times
- **Rationale**: More accurate than assuming fixed 4-hour intervals; includes Yankee Stadium exception

#### Usage Threshold
- **Maximum**: 7,200 entries/exits per 4-hour period
- **Calculation**: `USAGE_THRESHOLD_4HOURS = 7200`
- **Logic**: Represents 1 swipe every 2 seconds for 4 hours continuously
- **Rationale**: Anything higher is physically impossible

#### Counter Reset Handling
- **Method**: Clip negative differences to 0
- **When Applied**: When current count < previous count
- **Rationale**: Handles when cumulative counters reset to zero

#### First Reading Removal
- **Action**: Drop first reading from each turnstile
- **Rationale**: No previous value to calculate difference from

#### Output Year Filter
- **Years**: 2015-2019 only
- **Purpose**: Pre-pandemic baseline period
- **Note**: Even if processing 2010-2023 data, output is filtered

#### Missing Mapping Filter
- **Filter**: Drop records without Complex ID mapping
- **Impact**: Ensures all records can be aggregated to station level

## Modern Ridership Data Filters

### Stage: Ridership Staging (`scripts/local/stage_ridership_data.py`)

#### Transit Mode Filter
- **Allowed Modes**: `['subway']` only
- **Excluded**: bus, ferry, LIRR, Metro-North
- **Rationale**: Maintain consistency with turnstile data scope

#### Station Complex Filter
- **Filter**: Remove `station_complex_id == 502` (Tompkinsville)
- **Rationale**: Complex ID 502 is Tompkinsville on the Staten Island Railway (SIR), which is out of scope for NYC Subway analysis. Some modern ridership records appear with this Complex ID despite `transit_mode = 'subway'`; they are filtered during staging and reinforced in monthly calculations.

### Stage: Ridership Processing (`scripts/local/process_ridership_data.py`)

#### Data Normalization
- **Payment Method**: Convert to uppercase
- **Example**: "omny" → "OMNY", "metrocard" → "METROCARD"
- **Rationale**: Handle inconsistent capitalization

## Geographic Aggregation Filters

### PUMA Level Aggregation

#### PUMA Mapping Requirement
- **Filter**: Exclude stations without PUMA assignment
- **Impact**: Stations outside NYC boundaries
- **Rationale**: Cannot aggregate to neighborhood level without mapping

## Special Time Handling

### Midnight-Spanning Periods
- **Period**: 20:00–00:00 (8 PM to midnight)
- **Attribution**: Assigned to the date of the 20:00 reading (the period’s start)
- **Example**: 2024-01-15 20:00 to 2024-01-16 00:00 → attributed to 2024-01-15
- **Rationale**: Differences are computed between consecutive cumulative readings; the ridership between 20:00 and 00:00 properly belongs to the day anchored at the 20:00 start time (matches `process_turnstile_data.py`).

## 📊 Summary Table

| Filter Type | Value | Stage | Rationale |
|------------|-------|-------|-----------|
| Station Exclusion | "ORCHARD BEACH" | Staging | Not a real subway station |
| Station Complex | != 502 | All stages | Data quality issue |
| Division | BMT, IND, IRT only | Staging | Subway only |
| Date Range | ≥ Oct 18, 2014 | Staging | Modern format only |
| Time Intervals | :00:00, :30:00, :22:00 | Processing | Regular reporting + Yankee Stadium |
| Outlier Threshold | < 10% modal count | Processing | Remove broken turnstiles |
| Max Usage | 7,200 per 4hr | Processing | Physical impossibility |
| Baseline Years | 2015-2019 | Processing | Pre-pandemic period |
| Transit Mode | Subway only | Staging | Consistency |

## 🚨 Impact Analysis

### Records Removed (Typical)
- **Orchard Beach**: ~1,000 records
- **Outlier Turnstiles**: ~2-3% of turnstiles
- **Time Interval Filter**: ~60% of records (keeps 40%)
- **Threshold Violations**: <0.1% of records
- **Missing Mappings**: <0.5% of records

### Data Integrity
- **Validation**: Results match official MTA statistics within 1%
- **Coverage**: >99% of legitimate subway ridership captured
- **Quality**: Removes noise while preserving signal

## 🔧 Implementation Notes

### Adding New Filters
1. Document the filter value and rationale
2. Add to appropriate processing stage
3. Log number of records affected
4. Update this documentation

### Modifying Thresholds
- Usage threshold based on turnstile physical limitations
- Outlier threshold based on statistical analysis
- Time intervals based on MTA reporting patterns

### Special Cases
- **Complex ID 604 (161 St-Yankee Stadium)**: This station uniquely reports at :22 minutes past the hour instead of standard :00 or :30. Without the :22:00 filter, 100% of this major station's data would be excluded from analysis.

## 📝 References

- See [turnstile_data.md](turnstile_data.md) for data format details
- See [subway_stations.md](subway_stations.md) for station mapping information
- See processing script docstrings for implementation details