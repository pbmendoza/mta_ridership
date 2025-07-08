# Baseline Special Cases Configuration Guide

## Overview

The `references/baseline_special_cases.csv` file centralizes the configuration for MTA stations that require special handling during baseline calculations. This approach replaces hardcoded values in the processing scripts, making the system more maintainable and flexible.

## Why Special Cases?

The standard baseline calculation uses 2015-2019 data to establish pre-pandemic ridership patterns. However, some stations require different baseline periods because they were closed for a significant period during part of the baseline period.

## Configuration File Structure

### CSV Format
The configuration is stored in a CSV file with the following columns:

```csv
complex_id,baseline_years,station_name,reason,notes
328,2019,WTC-Cortlandt,Station closed from 9/11/2001...,Using single year...
475,"2018,2019",96 St-2 Ave,Second Avenue Subway Phase 1...,Excluded 2017...
```

**Column Order**: The first two columns (complex_id and baseline_years) are required and must contain valid values. The remaining columns are optional but recommended for documentation.

#### Field Descriptions

| Field | Required | Type | Purpose |
|-------|----------|------|---------|
| complex_id | Yes | Integer | Unique identifier for the station complex (1st column) |
| baseline_years | Yes | String | Comma-separated years (2nd column) |
| station_name | Recommended | String | Human-readable station name for logging (3rd column) |
| reason | Recommended | String | Explanation for special handling (4th column) |
| notes | Optional | String | Additional context or warnings (5th column) |

### Multiple Years Format
- Single year: `2019`
- Multiple years: `"2018,2019"` (use quotes if comma-separated)
- Years are parsed as integers during processing

### Default Baseline Years  
The default baseline years [2015, 2016, 2017, 2018, 2019] are defined as a constant in the `calculate_baseline.py` script:
```python
DEFAULT_BASELINE_YEARS = [2015, 2016, 2017, 2018, 2019]
```

## How Baseline Calculation Works

### 1. Configuration Loading
The `calculate_baseline.py` script loads the CSV configuration at startup:
```python
config = load_special_cases_config(base_dir, logger)
special_cases = config['special_cases']  # Dictionary created from CSV
default_years = DEFAULT_BASELINE_YEARS   # Constant in script
```

### 2. Station Processing

#### Regular Stations
- Use all years specified in `default_baseline_years`
- Monthly totals are divided by the number of years (typically 5)
- Example: Station with 500,000 January entries across 2015-2019
  - Monthly baseline = 500,000 ÷ 5 = 100,000 entries

#### Special Case Stations
- Use only years specified in their `baseline_years` array
- Monthly totals are divided by the number of specified years
- Example: WTC-Cortlandt with 300,000 January entries in 2019
  - Monthly baseline = 300,000 ÷ 1 = 300,000 entries

### 3. Calculation Formula
For any station:
```
Monthly Baseline = Sum of entries/exits for month across specified years
                   ÷ Number of specified years
```

## Adding a New Special Case

### Step 1: Edit the Configuration
Add a new row to the CSV file:

```csv
123,"2018,2019",Example Station,Station underwent major renovation 2015-2017,Pre-renovation ridership patterns differ significantly
```

**Note**: Use quotes around multiple years (e.g., `"2018,2019"`)

### Step 2: Validate
- Ensure the Complex ID is correct (check `stations_complexes_official.csv`)
- Verify the years contain data for that station
- Provide clear reason and notes for future reference

### Step 3: Run the Script
No code changes needed! The script automatically processes the new configuration.

## Removing a Special Case

Simply delete the station's row from the CSV file. The station will then use the default baseline years.

## Best Practices

### 1. Document Your Changes
- Provide detailed `reason` for each special case
- Use `notes` for warnings or limitations
- Consider adding a comment in the script when making significant changes

### 2. Year Selection Guidelines
- Include only years with complete data
- For new stations, exclude the opening year if partial
- For renovated stations, use post-renovation years
- Consider ridership stabilization periods

### 3. Testing
After modifications:
1. Run `calculate_baseline.py`
2. Check logs for special case processing messages
3. Verify output files contain expected values

### 4. CSV Editing Tips
- Use a spreadsheet application (Excel, Google Sheets) for easier editing
- Remember to save as CSV (not Excel format)
- Use quotes for comma-separated years
- Avoid extra commas or special characters

## Data Validation

The script performs the following validation when loading the configuration:

1. **Required Columns**: Checks that `complex_id` and `baseline_years` columns exist
2. **Required Values**: Ensures neither `complex_id` nor `baseline_years` are empty
3. **Data Types**: 
   - `complex_id` must be a valid integer
   - `baseline_years` must be comma-separated integers
4. **Error Reporting**: Provides row numbers for any validation errors

## Related Files

- `scripts/calculate_baseline.py` - Processes this configuration
- `references/baseline_special_cases.csv` - The configuration file
- `references/docs/special_cases/wtc_cortlandt_special_case.md` - Detailed WTC history
- `references/docs/special_cases/second_avenue_subway_special_case.md` - Second Ave details
- `results/baseline/` - Output directory for baseline calculations