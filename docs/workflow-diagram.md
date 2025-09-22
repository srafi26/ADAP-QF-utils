# Workflow Diagrams

## Complete Contributor Deletion Workflow

```mermaid
flowchart TD
    A[Start] --> B[Load Configuration]
    B --> C[Load Contributors from CSV]
    C --> D[Create Backup]
    D --> E[Phase 1: Mask Elasticsearch Data]
    E --> F[Phase 2: Mask ClickHouse Data]
    F --> G[Phase 3: Delete PostgreSQL Data]
    G --> H[Phase 4: Delete S3 Files]
    H --> I[Phase 5: Clear Redis Sessions]
    I --> J[Generate Report]
    J --> K[End]
    
    E --> E1[Search project-* indices]
    E1 --> E2[Mask email addresses]
    E2 --> E3[Mask worker IDs]
    E3 --> E4[Update documents]
    
    F --> F1[Query unit_metrics tables]
    F1 --> F2[Mask email addresses]
    F2 --> F3[Handle Kafka tables]
    
    G --> G1[Delete job mappings]
    G1 --> G2[Delete contributor records]
    G2 --> G3[Mask PII data]
    
    H --> H1[List S3 objects]
    H1 --> H2[Delete contributor files]
    
    I --> I1[Clear auth cache]
    I1 --> I2[Clear session data]
    I2 --> I3[Clear job cache]
```

## API Testing Workflow

```mermaid
flowchart TD
    A[Start API Testing] --> B[Parse Job URL]
    B --> C[Load Contributors from CSV]
    C --> D[For Each Contributor]
    D --> E[Test Fetch API]
    E --> F{Fetch Successful?}
    F -->|Yes| G[Test Commit API]
    F -->|No| H[Log Failure]
    G --> I{Commit Successful?}
    I -->|Yes| J[Log Success]
    I -->|No| K[Log Commit Failure]
    H --> L[Next Contributor]
    J --> L
    K --> L
    L --> M{More Contributors?}
    M -->|Yes| D
    M -->|No| N[Generate Report]
    N --> O[End]
```

## Data Fetching Workflow

```mermaid
flowchart TD
    A[Start Fetching] --> B[Connect to PostgreSQL]
    B --> C[Query Inactive Contributors]
    C --> D[Apply Filters]
    D --> E[Sort by Last Activity]
    E --> F[Limit Results]
    F --> G[Export to CSV]
    G --> H[Create Backup]
    H --> I[End]
    
    C --> C1[Status = INACTIVE]
    C1 --> C2[Last activity > threshold]
    C2 --> C3[Project count criteria]
    
    D --> D1[Days inactive filter]
    D1 --> D2[Min projects filter]
    D2 --> D3[Max contributors filter]
```

## Verification Workflow

```mermaid
flowchart TD
    A[Start Verification] --> B[Check Elasticsearch]
    B --> C[Search for DELETED_USER]
    C --> D[Check ClickHouse]
    D --> E[Query masked records]
    E --> F[Test API Access]
    F --> G[Verify Contributors Kicked Out]
    G --> H[Generate Verification Report]
    H --> I[End]
    
    B --> B1[Search project-* indices]
    B1 --> B2[Count DELETED_USER entries]
    
    D --> D1[Query unit_metrics]
    D1 --> D2[Count masked emails]
    
    F --> F1[Test fetch API]
    F1 --> F2[Should return SYSTEM_ERROR]
```

## Error Handling Workflow

```mermaid
flowchart TD
    A[Operation Starts] --> B[Try Operation]
    B --> C{Success?}
    C -->|Yes| D[Log Success]
    C -->|No| E[Log Error]
    E --> F[Check Error Type]
    F --> G{Critical Error?}
    G -->|Yes| H[Rollback Transaction]
    G -->|No| I[Continue Processing]
    H --> J[Log Rollback]
    I --> K[Log Warning]
    J --> L[End with Error]
    K --> M[Continue Next Operation]
    D --> N[Continue Next Operation]
    M --> O{More Operations?}
    N --> O
    O -->|Yes| B
    O -->|No| P[End Successfully]
```

## Environment Setup Workflow

```mermaid
flowchart TD
    A[Start Setup] --> B[Check Python Dependencies]
    B --> C[Verify Configuration Files]
    C --> D[Test Database Connection]
    D --> E[Test Elasticsearch Connection]
    E --> F[Setup ClickHouse Port-Forward]
    F --> G[Test ClickHouse Connection]
    G --> H[Verify S3 Access]
    H --> I[Test Redis Connection]
    I --> J[All Connections OK?]
    J -->|Yes| K[Setup Complete]
    J -->|No| L[Fix Connection Issues]
    L --> M[Retry Setup]
    M --> J
    K --> N[Ready to Run Scripts]
```

## Safety Checks Workflow

```mermaid
flowchart TD
    A[Start Safety Checks] --> B[Check Dry Run Mode]
    B --> C[Verify CSV Format]
    C --> D[Validate Contributor IDs]
    D --> E[Check Configuration]
    E --> F[Test All Connections]
    F --> G[Create Backup]
    G --> H[All Checks Pass?]
    H -->|Yes| I[Proceed with Operation]
    H -->|No| J[Abort with Error]
    I --> K[Execute Operation]
    J --> L[End with Error]
    K --> M[End Successfully]
```
