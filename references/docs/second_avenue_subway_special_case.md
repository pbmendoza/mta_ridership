# Second Avenue Subway Special Case

## Overview

The Second Avenue Subway stations (Complex IDs: 475, 476, 477) require special handling in baseline calculations due to their recent opening as part of the first major subway expansion in New York City in over 50 years.

## Station Details

- **96 St-2 Ave** (Complex ID: 475)
- **86 St-2 Ave** (Complex ID: 476) 
- **72 St-2 Ave** (Complex ID: 477)

## Historical Context

### A Century in the Making

The Second Avenue Subway has an extraordinary history spanning nearly 100 years:

- **1920**: Originally proposed as part of a massive expansion of what would become the Independent Subway System (IND)
- **Multiple attempts**: Construction started and stopped several times throughout the 20th century due to funding issues
- **$4.5 billion**: Final cost of Phase 1, making it one of the most expensive transit projects per mile

### Phase 1 Opening

- **Ceremonial Opening**: December 31, 2016 at 10:30 PM
  - First train carried Governor Cuomo, Mayor de Blasio, and other officials
  - Departed from 72nd Street heading to 96th Street
  
- **Revenue Service**: January 1, 2017 at noon
  - Stations opened to the public at 11:45 AM
  - First revenue train arrived at approximately noon
  - 48,200 passengers entered the new stations on opening day

### Impact on Ridership

The opening immediately affected ridership patterns:
- Significant decrease in ridership at nearby Lexington Avenue Line stations (68th, 77th, 86th, 96th Streets)
- Provided much-needed relief to the overcrowded 4/5/6 lines
- Served the far Upper East Side, which had limited subway access

## Baseline Calculation Approach

### The Challenge

Standard baseline calculations use 2015-2019 data, but the Second Avenue Subway stations present unique considerations:

- **2015-2016**: No data (stations not yet open)
- **2017**: First full year of operation - ridership still ramping up
- **2018-2019**: More stable ridership patterns established

### Implementation

In `scripts/calculate_baseline.py`, these stations are processed separately:

1. **Regular stations**: Use 2015-2019 data, divide by 5 for monthly average
2. **Second Avenue Subway**: Use only 2018-2019 data, divide by 2 for monthly average

```python
# Special case station constants
SECOND_AVE_SUBWAY_COMPLEX_IDS = [475, 476, 477]  # Opened Jan 2017

# Process Second Avenue Subway stations with 2-year average
df_sas = df[(df['Complex ID'].isin(SECOND_AVE_SUBWAY_COMPLEX_IDS)) & 
            (df['YEAR'].isin([2018, 2019]))]
monthly_sas['ENTRIES'] = monthly_sas['ENTRIES'] / 2
monthly_sas['EXITS'] = monthly_sas['EXITS'] / 2
```

### Rationale

Using 2018-2019 data provides a more accurate baseline because:
- Allows one full year (2017) for ridership patterns to stabilize
- Captures "normal" usage after initial novelty period
- Reflects established commuting patterns and neighborhood integration

## Annual Ridership Summary (2018-2019 Average)

| Station | Complex ID | Annual Entries | Annual Exits |
|---------|------------|----------------|--------------|
| 96 St-2 Ave | 475 | 6,126,146 | 4,410,486 |
| 86 St-2 Ave | 476 | 8,240,906 | 5,530,555 |
| 72 St-2 Ave | 477 | 9,309,472 | 8,158,929 |

## Considerations

1. **Ridership Growth**: These stations may still experience organic growth as:
   - Neighborhood development continues
   - More residents adapt their commuting patterns
   - Phase 2 construction (extending to 125th Street) may affect ridership

2. **Baseline Accuracy**: The 2-year baseline provides a reasonable approximation but may underestimate long-term ridership potential

3. **Future Phases**: 
   - Phase 2 (to 125th Street) is currently under construction
   - Future phases planned to extend further north and south
   - Each phase opening will likely affect ridership at existing stations

## Related Files

- `scripts/calculate_baseline.py`: Contains special handling logic
- `scripts/process_turnstile_data.py`: Processes raw turnstile data
- `data/processed/turnstile/daily_ridership.csv`: Contains the 2017-2019 data
- `references/docs/wtc_cortlandt_special_case.md`: Similar special case documentation