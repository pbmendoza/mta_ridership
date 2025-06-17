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

# Step 1: Stage Raw Data (Parallel)
print_header "Step 1: Staging Raw Data"
print_step "Running staging scripts in parallel..."

# Check if scripts exist
check_script "scripts/stage_turnstile_data.py"
check_script "scripts/stage_ridership_data.py"

# Run staging scripts in parallel
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

# Check for logs
if [ -d "logs" ]; then
    echo -e "\n${BLUE}Check logs for detailed information:${NC}"
    echo "  • logs/"
fi