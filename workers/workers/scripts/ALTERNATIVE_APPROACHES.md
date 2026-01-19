# Alternative Approaches for File Info Population

## Recommended Approach: Database-Driven Task Management System

### Why This Approach?

Based on the requirements analysis, a database-driven approach addresses all key concerns:

1. **Crash Recovery**: Database state survives system crashes
2. **UI Integration**: Easy to build monitoring interface  
3. **Scalability**: Multiple workers can process different batches
4. **Automation**: Background service runs continuously
5. **Flexibility**: Can pause/resume/modify during execution

### Database Schema

```sql
-- Track file info population campaigns
CREATE TABLE file_info_population_campaigns (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE', -- ACTIVE, PAUSED, COMPLETED, CANCELLED
    batch_size INTEGER DEFAULT 10,
    max_download_size_tb FLOAT DEFAULT 10,
    download_dir VARCHAR(500),
    total_datasets INTEGER DEFAULT 0,
    processed_datasets INTEGER DEFAULT 0,
    failed_datasets INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    created_by VARCHAR(100),
    
    CONSTRAINT valid_status CHECK (status IN ('ACTIVE', 'PAUSED', 'COMPLETED', 'CANCELLED'))
);

-- Track individual dataset processing tasks
CREATE TABLE file_info_population_tasks (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER REFERENCES file_info_population_campaigns(id) ON DELETE CASCADE,
    dataset_id INTEGER REFERENCES datasets(id),
    dataset_name VARCHAR(200),
    status VARCHAR(20) DEFAULT 'PENDING',
    -- PENDING, DOWNLOADING, DOWNLOADED, WORKFLOW_STARTED, PROCESSING, COMPLETED, FAILED
    batch_number INTEGER,
    priority INTEGER DEFAULT 0, -- Higher numbers = higher priority
    
    -- Download tracking
    download_path VARCHAR(500) NULL,
    download_size_bytes BIGINT NULL,
    download_started_at TIMESTAMP NULL,
    download_completed_at TIMESTAMP NULL,
    
    -- Workflow tracking  
    workflow_id VARCHAR(50) NULL,
    workflow_started_at TIMESTAMP NULL,
    workflow_completed_at TIMESTAMP NULL,
    
    -- Error handling
    error_message TEXT NULL,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    last_retry_at TIMESTAMP NULL,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    
    CONSTRAINT valid_task_status CHECK (status IN (
        'PENDING', 'DOWNLOADING', 'DOWNLOADED', 'WORKFLOW_STARTED', 
        'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED'
    )),
    CONSTRAINT unique_dataset_per_campaign UNIQUE(campaign_id, dataset_id)
);

-- Index for performance
CREATE INDEX idx_file_info_tasks_status ON file_info_population_tasks(status);
CREATE INDEX idx_file_info_tasks_campaign ON file_info_population_tasks(campaign_id);
CREATE INDEX idx_file_info_tasks_batch ON file_info_population_tasks(batch_number);
```

### Background Service Architecture

```python
class FileInfoPopulationService:
    """Background service that continuously processes file info population tasks."""
    
    def __init__(self):
        self.running = True
        self.current_campaign = None
        self.download_semaphore = Semaphore(3)  # Max 3 concurrent downloads
        
    def run(self):
        """Main service loop."""
        while self.running:
            try:
                # Get active campaign
                campaign = self.get_active_campaign()
                if not campaign:
                    time.sleep(30)  # Wait for campaign to be created
                    continue
                
                # Check if we need to pause
                if campaign['status'] == 'PAUSED':
                    time.sleep(10)
                    continue
                
                # Process next batch
                self.process_next_batch(campaign)
                
            except Exception as e:
                logger.error(f"Service error: {e}")
                time.sleep(60)  # Wait before retrying
    
    def get_active_campaign(self):
        """Get the currently active campaign."""
        return db.query("""
            SELECT * FROM file_info_population_campaigns 
            WHERE status IN ('ACTIVE', 'PAUSED')
            ORDER BY created_at DESC LIMIT 1
        """)
    
    def process_next_batch(self, campaign):
        """Process the next batch of datasets."""
        # Get pending tasks for next batch
        pending_tasks = self.get_pending_tasks(campaign['id'])
        
        if not pending_tasks:
            # Check if campaign is complete
            if self.is_campaign_complete(campaign['id']):
                self.complete_campaign(campaign['id'])
            return
        
        # Check space constraints
        if not self.has_space_for_batch(pending_tasks):
            logger.warning("Not enough space for next batch, waiting...")
            time.sleep(300)  # Wait 5 minutes
            return
        
        # Process batch concurrently
        with ThreadPoolExecutor(max_workers=campaign['batch_size']) as executor:
            futures = []
            for task in pending_tasks:
                future = executor.submit(self.process_task, task)
                futures.append(future)
            
            # Wait for all tasks to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Task processing error: {e}")
    
    def process_task(self, task):
        """Process a single dataset task."""
        task_id = task['id']
        dataset_id = task['dataset_id']
        
        try:
            # Update status to downloading
            self.update_task_status(task_id, 'DOWNLOADING')
            
            # Download dataset
            download_path = self.download_dataset(dataset_id)
            self.update_task(task_id, {
                'status': 'DOWNLOADED',
                'download_path': download_path,
                'download_completed_at': 'NOW()'
            })
            
            # Start workflow
            workflow_id = self.start_workflow(dataset_id)
            self.update_task(task_id, {
                'status': 'WORKFLOW_STARTED',
                'workflow_id': workflow_id,
                'workflow_started_at': 'NOW()'
            })
            
            # Monitor workflow completion (async)
            self.schedule_workflow_monitoring(task_id, workflow_id)
            
        except Exception as e:
            self.handle_task_error(task_id, str(e))
    
    def schedule_workflow_monitoring(self, task_id, workflow_id):
        """Schedule monitoring of workflow completion."""
        # This could be a separate background thread or Celery task
        threading.Thread(
            target=self.monitor_workflow_completion,
            args=(task_id, workflow_id),
            daemon=True
        ).start()
    
    def monitor_workflow_completion(self, task_id, workflow_id):
        """Monitor workflow until completion."""
        while True:
            workflow_status = api.get_workflow_status(workflow_id)
            
            if workflow_status in ['SUCCESS']:
                self.update_task(task_id, {
                    'status': 'COMPLETED',
                    'workflow_completed_at': 'NOW()',
                    'completed_at': 'NOW()'
                })
                self.cleanup_task_files(task_id)
                break
                
            elif workflow_status in ['FAILURE', 'REVOKED']:
                self.update_task(task_id, {
                    'status': 'FAILED',
                    'error_message': f'Workflow failed with status: {workflow_status}',
                    'workflow_completed_at': 'NOW()'
                })
                self.cleanup_task_files(task_id)
                break
                
            time.sleep(60)  # Check every minute
```

### Web UI Components

#### 1. Campaign Management Page
```javascript
// React component for campaign management
const CampaignManager = () => {
    const [campaigns, setCampaigns] = useState([]);
    const [currentCampaign, setCurrentCampaign] = useState(null);
    
    const createCampaign = async (config) => {
        const response = await api.post('/file-info-campaigns', config);
        // Refresh campaigns list
    };
    
    const pauseCampaign = async (campaignId) => {
        await api.patch(`/file-info-campaigns/${campaignId}`, { status: 'PAUSED' });
    };
    
    const resumeCampaign = async (campaignId) => {
        await api.patch(`/file-info-campaigns/${campaignId}`, { status: 'ACTIVE' });
    };
    
    return (
        <div>
            <CampaignList campaigns={campaigns} onPause={pauseCampaign} onResume={resumeCampaign} />
            <CampaignCreator onCreate={createCampaign} />
            {currentCampaign && <CampaignDashboard campaign={currentCampaign} />}
        </div>
    );
};
```

#### 2. Real-time Progress Dashboard
```javascript
const ProgressDashboard = ({ campaignId }) => {
    const [stats, setStats] = useState({});
    const [tasks, setTasks] = useState([]);
    
    useEffect(() => {
        // WebSocket connection for real-time updates
        const ws = new WebSocket(`/ws/campaigns/${campaignId}/progress`);
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            setStats(data.stats);
            setTasks(data.tasks);
        };
        
        return () => ws.close();
    }, [campaignId]);
    
    return (
        <div>
            <ProgressStats stats={stats} />
            <TaskList tasks={tasks} />
            <SpaceUsageChart />
        </div>
    );
};
```

### API Endpoints

```python
# FastAPI endpoints for campaign management
@app.post("/api/file-info-campaigns")
async def create_campaign(campaign: CampaignCreate):
    """Create a new file info population campaign."""
    # 1. Validate parameters
    # 2. Create campaign record
    # 3. Identify datasets needing file info
    # 4. Create tasks for each dataset
    # 5. Return campaign details
    
@app.patch("/api/file-info-campaigns/{campaign_id}")
async def update_campaign(campaign_id: int, update: CampaignUpdate):
    """Update campaign status (pause/resume/cancel)."""
    
@app.get("/api/file-info-campaigns/{campaign_id}/progress")
async def get_campaign_progress(campaign_id: int):
    """Get detailed progress information."""
    
@app.post("/api/file-info-campaigns/{campaign_id}/tasks/{task_id}/retry")
async def retry_failed_task(campaign_id: int, task_id: int):
    """Retry a failed task."""
    
@app.websocket("/ws/campaigns/{campaign_id}/progress")
async def campaign_progress_websocket(websocket: WebSocket, campaign_id: int):
    """WebSocket for real-time progress updates."""
```

### Advantages of This Approach

1. **Reliability**: Database state survives crashes, network issues, etc.
2. **Visibility**: Full visibility into progress via web UI
3. **Control**: Can pause, resume, modify parameters during execution
4. **Scalability**: Can run multiple background workers
5. **Maintainability**: Clear separation of concerns
6. **Flexibility**: Easy to add new features (notifications, scheduling, etc.)
7. **Recovery**: Can retry individual failed datasets
8. **Monitoring**: Built-in progress tracking and statistics

### Migration from Script Approach

The existing script can be easily adapted:

1. **Phase 1**: Add database tables and basic background service
2. **Phase 2**: Add web UI for monitoring
3. **Phase 3**: Add advanced features (scheduling, notifications, etc.)

This approach provides the best balance of automation, reliability, and user control while being maintainable and scalable.

## Alternative Approach 2: Celery-Based Task Queue

If you prefer to leverage existing Celery infrastructure:

### Task Structure
```python
@celery.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def download_and_process_dataset(self, dataset_id, campaign_id):
    """Download dataset and start file info workflow."""
    
@celery.task(bind=True)
def batch_coordinator(self, campaign_id, batch_datasets):
    """Coordinate processing of a batch of datasets."""
    
@celery.task(bind=True)
def campaign_manager(self, campaign_id):
    """Manage overall campaign execution."""
```

### Benefits
- Leverages existing Celery infrastructure
- Built-in retry and error handling
- Flower UI for monitoring
- Distributed processing capability

### Drawbacks
- Less fine-grained control than database approach
- Harder to implement custom UI
- Task state limited to Celery's model

## Alternative Approach 3: Kubernetes Jobs

For cloud-native environments:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: file-info-population-batch-1
spec:
  parallelism: 3
  completions: 10
  template:
    spec:
      containers:
      - name: file-info-processor
        image: bioloop/file-info-processor
        env:
        - name: BATCH_NUMBER
          value: "1"
        - name: CAMPAIGN_ID
          value: "123"
```

### Benefits
- Cloud-native scaling
- Built-in job management
- Resource isolation
- Kubernetes monitoring tools

## Conclusion

The **Database-Driven Task Management System** provides the best balance of:
- Reliability and crash recovery
- User visibility and control  
- Scalability and maintainability
- Integration with existing Bioloop architecture

This approach can start simple and evolve to include advanced features like scheduling, notifications, and distributed processing as needed.
