#!/bin/bash

# MTA Ridership Data Processing Pipeline
# ======================================
# This script runs the complete data processing pipeline in the correct order.
# It processes both historical turnstile data and modern ridership data,
# then combines them with baseline comparisons.

# Exit on any error
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored headers
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

# Function to print step info
print_step() {
    echo -e "${GREEN}▶ $1${NC}"
}

# Function to print warnings
print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Function to print errors
print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Function to check if script exists
check_script() {
    if [ ! -f "$1" ]; then
        print_error "Script not found: $1"
        exit 1
    fi
}

# Start timing
START_TIME=$(date +%s)

print_header "MTA Ridership Data Processing Pipeline"
echo "Starting at: $(date)"

# Check Python
print_step "Checking Python installation..."
if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    print_error "Python is not installed!"
    exit 1
fi

# Use python3 if available, otherwise python
PYTHON_CMD=$(command -v python3 || command -v python)
echo "Using Python: $PYTHON_CMD"

# Step 0: Clean intermediate and output directories
print_header "Step 0: Cleaning Intermediate & Output Directories"
print_step "Removing previous staging, processed, and results files..."
print_step "(If the pipeline fails, this run's output will still be available for investigation)"

# Clean ridership staging (but preserve turnstile combined file — it's expensive to regenerate)
if [ -d "data/staging/ridership" ]; then
    STAGING_COUNT=$(find data/staging/ridership -type f -name "*.csv" | wc -l | tr -d ' ')
    rm -f data/staging/ridership/*.csv
    print_step "Cleaned data/staging/ridership/ ($STAGING_COUNT files)"
fi

# Clean processed ridership
if [ -d "data/processed/ridership" ]; then
    PROCESSED_COUNT=$(find data/processed/ridership -type f -name "*.csv" | wc -l | tr -d ' ')
    rm -f data/processed/ridership/*.csv
    print_step "Cleaned data/processed/ridership/ ($PROCESSED_COUNT files)"
fi

# Clean results
for dir in results/baseline results/ridership results/final; do
    if [ -d "$dir" ]; then
        RESULT_COUNT=$(find "$dir" -type f -name "*.csv" | wc -l | tr -d ' ')
        rm -f "$dir"/*.csv
        print_step "Cleaned $dir/ ($RESULT_COUNT files)"
    fi
done

print_step "Cleanup complete — starting fresh"

# Step 1: Stage Raw Data
print_header "Step 1: Staging Raw Data"

# Check if turnstile combined file already exists
TURNSTILE_COMBINED="data/staging/turnstile/turnstile_combined.csv"
SKIP_TURNSTILE=false

if [ -f "$TURNSTILE_COMBINED" ]; then
    FILESIZE=$(ls -lh "$TURNSTILE_COMBINED" | awk '{print $5}')
    FILEDATE=$(stat -f "%Sm" -t "%Y-%m-%d" "$TURNSTILE_COMBINED" 2>/dev/null || date -r "$TURNSTILE_COMBINED" '+%Y-%m-%d' 2>/dev/null || echo "unknown")
    print_step "Found existing turnstile combined file:"
    echo "  📁 File: $TURNSTILE_COMBINED"
    echo "  📏 Size: $FILESIZE"
    echo "  📅 Date: $FILEDATE"
    print_step "Skipping turnstile staging (using existing file)"
    SKIP_TURNSTILE=true
else
    print_warning "Turnstile combined file not found - will create it"
fi

# Check if scripts exist
check_script "scripts/stage_ridership_data.py"
if [ "$SKIP_TURNSTILE" = false ]; then
    check_script "scripts/stage_turnstile_data.py"
fi

# Run staging scripts
if [ "$SKIP_TURNSTILE" = true ]; then
    # Only run ridership staging
    print_step "Staging ridership data..."
    $PYTHON_CMD scripts/stage_ridership_data.py
    if [ $? -ne 0 ]; then
        print_error "Ridership staging failed!"
        exit 1
    fi
else
    # Run both staging scripts in parallel
    print_step "Running staging scripts in parallel..."
    
    (
        print_step "Staging turnstile data..."
        $PYTHON_CMD scripts/stage_turnstile_data.py 2>&1 | sed 's/^/  [Turnstile] /'
    ) &
    PID1=$!
    
    (
        print_step "Staging ridership data..."
        $PYTHON_CMD scripts/stage_ridership_data.py 2>&1 | sed 's/^/  [Ridership] /'
    ) &
    PID2=$!
    
    # Wait for both to complete
    wait $PID1
    TURNSTILE_STAGE_STATUS=$?
    wait $PID2
    RIDERSHIP_STAGE_STATUS=$?
    
    if [ $TURNSTILE_STAGE_STATUS -ne 0 ]; then
        print_error "Turnstile staging failed!"
        exit 1
    fi
    
    if [ $RIDERSHIP_STAGE_STATUS -ne 0 ]; then
        print_error "Ridership staging failed!"
        exit 1
    fi
fi

print_step "Staging completed successfully!"

# Step 2: Process Staged Data (Parallel)
print_header "Step 2: Processing Staged Data"
print_step "Running processing scripts in parallel..."

check_script "scripts/process_turnstile_data.py"
check_script "scripts/process_ridership_data.py"

# Run processing scripts in parallel
(
    print_step "Processing turnstile data..."
    print_warning "This may take several minutes due to large data volume..."
    $PYTHON_CMD scripts/process_turnstile_data.py 2>&1 | sed 's/^/  [Turnstile] /'
) &
PID1=$!

(
    print_step "Processing ridership data..."
    $PYTHON_CMD scripts/process_ridership_data.py 2>&1 | sed 's/^/  [Ridership] /'
) &
PID2=$!

# Wait for both to complete
wait $PID1
TURNSTILE_PROCESS_STATUS=$?
wait $PID2
RIDERSHIP_PROCESS_STATUS=$?

if [ $TURNSTILE_PROCESS_STATUS -ne 0 ]; then
    print_error "Turnstile processing failed!"
    exit 1
fi

if [ $RIDERSHIP_PROCESS_STATUS -ne 0 ]; then
    print_error "Ridership processing failed!"
    exit 1
fi

print_step "Processing completed successfully!"

# Step 3: Calculate Baseline
print_header "Step 3: Calculating Historical Baseline (2015-2019)"
check_script "scripts/calculate_baseline.py"

print_step "Calculating baseline from historical data..."
$PYTHON_CMD scripts/calculate_baseline.py
if [ $? -ne 0 ]; then
    print_error "Baseline calculation failed!"
    exit 1
fi

# Step 4: Calculate Modern Ridership
print_header "Step 4: Calculating Modern Ridership Metrics"
check_script "scripts/calculate_ridership.py"

print_step "Calculating ridership metrics with OMNY adoption..."
$PYTHON_CMD scripts/calculate_ridership.py
if [ $? -ne 0 ]; then
    print_error "Ridership calculation failed!"
    exit 1
fi

# Step 5: Generate Final Output
print_header "Step 5: Generating Final Analysis"
check_script "scripts/calculate_final.py"

print_step "Merging ridership with baseline comparisons..."
$PYTHON_CMD scripts/calculate_final.py
if [ $? -ne 0 ]; then
    print_error "Final calculation failed!"
    exit 1
fi

# Step 6: Enrich Final Data with Names
print_header "Step 6: Enriching Final Data"
check_script "scripts/enrich_final_data.py"

print_step "Adding PUMA and station names, and sorting final output..."
$PYTHON_CMD scripts/enrich_final_data.py
if [ $? -ne 0 ]; then
    print_error "Data enrichment failed!"
    exit 1
fi

# Calculate total time
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

# Success message
print_header "Pipeline Completed Successfully! ✨"
echo "Total time: ${MINUTES} minutes ${SECONDS} seconds"
echo "Completed at: $(date)"

echo -e "\n${GREEN}Output files generated:${NC}"
echo "  • results/baseline/         - Historical baseline (2015-2019)"
echo "  • results/ridership/        - Modern ridership with OMNY metrics"
echo "  • results/final/            - Final analysis with baseline comparisons"

echo -e "\n${GREEN}Key files:${NC}"
echo "  • results/final/monthly_ridership_station.csv"
echo "  • results/final/monthly_ridership_puma.csv"
echo "  • results/final/monthly_ridership_nyc.csv"

# Optional: Run utility scripts
echo -e "\n${YELLOW}Optional utility scripts available:${NC}"
echo "  • python scripts/add_puma_to_stations.py     - Map stations to PUMA boundaries"
echo "  • python scripts/extract_unique_stations_turnstile.py - Extract station metadata"

# Note about regenerating turnstile data
echo -e "\n${YELLOW}Note about turnstile data:${NC}"
echo "  • Historical turnstile data (2014-2023) is pre-combined for efficiency"
echo "  • To regenerate: rm data/staging/turnstile/turnstile_combined.csv"
echo "  • Then run: python scripts/stage_turnstile_data.py"

# Check for logs
if [ -d "logs" ]; then
    echo -e "\n${BLUE}Check logs for detailed information:${NC}"
    echo "  • logs/"
fi