#!/bin/bash

# Contributor Deletion Script
# ===========================
# This script runs the complete contributor deletion process

set -e  # Exit on any error

# Configuration
CONFIG_FILE="${HOME}/config_integration.ini"
SAMPLE_SIZE=10
SKIP_REDIS=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dry-run          Run in dry-run mode (safe to test)"
    echo "  --execute          Actually perform deletion"
    echo "  --sample-size N    Number of contributors to process (default: 10)"
    echo "  --skip-redis       Skip Redis session clearing"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --dry-run --sample-size 5     # Test with 5 contributors"
    echo "  $0 --execute --sample-size 10    # Delete 10 contributors"
    echo "  $0 --execute --skip-redis        # Delete without Redis clearing"
}

# Parse command line arguments
DRY_RUN=true
EXECUTE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            EXECUTE=false
            shift
            ;;
        --execute)
            DRY_RUN=false
            EXECUTE=true
            shift
            ;;
        --sample-size)
            SAMPLE_SIZE="$2"
            shift 2
            ;;
        --skip-redis)
            SKIP_REDIS=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate arguments
if [[ "$DRY_RUN" == false && "$EXECUTE" == false ]]; then
    print_error "You must specify either --dry-run or --execute"
    show_usage
    exit 1
fi

# Check if config file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    print_error "Config file not found: $CONFIG_FILE"
    exit 1
fi

# Check if Python script exists
if [[ ! -f "run_deletion.py" ]]; then
    print_error "run_deletion.py not found in current directory"
    exit 1
fi

# Show configuration
print_info "Contributor Deletion Configuration:"
echo "  📁 Config file: $CONFIG_FILE"
echo "  🌍 Environment: Integration"
echo "  ⚙️  Mode: $([ "$DRY_RUN" == true ] && echo "DRY-RUN" || echo "EXECUTE")"
echo "  👥 Sample size: $SAMPLE_SIZE"
echo "  🔴 Skip Redis: $SKIP_REDIS"
echo ""

# Confirm execution
if [[ "$EXECUTE" == true ]]; then
    print_warning "⚠️  WARNING: This will actually delete contributors from the system!"
    print_warning "⚠️  Make sure you have backups and this is what you want to do."
    echo ""
    read -p "Are you sure you want to continue? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        print_info "Operation cancelled by user"
        exit 0
    fi
fi

# Run the deletion
print_info "Starting contributor deletion process..."

if [[ "$SKIP_REDIS" == true ]]; then
    python3 run_deletion.py \
        --config "$CONFIG_FILE" \
        --integration \
        $([ "$EXECUTE" == true ] && echo "--execute" || echo "") \
        --skip-redis \
        --sample-size "$SAMPLE_SIZE"
else
    python3 run_deletion.py \
        --config "$CONFIG_FILE" \
        --integration \
        $([ "$EXECUTE" == true ] && echo "--execute" || echo "") \
        --sample-size "$SAMPLE_SIZE"
fi

# Check exit code
if [[ $? -eq 0 ]]; then
    print_success "Contributor deletion process completed successfully!"
else
    print_error "Contributor deletion process failed!"
    exit 1
fi
