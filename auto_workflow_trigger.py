# auto_workflow_trigger.py ──────────────────────────────────────────────
"""
Automatic workflow trigger that monitors the "Startup Submissions" table
for records where "Run Match" is set to True and automatically runs
workflow_matching.py when detected.
"""

import time
import subprocess
import sys
import os
import fcntl
import atexit
from pyairtable import Api
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ── Airtable Configuration ────────────────────────────────────────────
AIRTABLE_API_KEY = "patPnlxR05peVEnUc.e5a8cfe5a3f88676da4b3c124c99ed46026b4f869bb5b6a3f54cd45db17fd58f"
BASE_ID = "app768aQ07mCJoyu8"
STARTUP_TABLE = "Startup Submissions"

# ── Initialize Airtable API ────────────────────────────────────────────
api = Api(AIRTABLE_API_KEY)
base = api.base(BASE_ID)
startup_tbl = base.table(STARTUP_TABLE)

def is_run_match_enabled(value):
    """Check if Run Match field is set to true (accepts True, 'true', 1, '1')"""
    if value is True or value == 'true' or value == 1 or value == '1':
        return True
    if isinstance(value, str) and value.strip().lower() == 'true':
        return True
    return False

def get_pending_matches():
    """Get startup records that need workflow matching."""
    try:
        # Get all records where Run Match is true
        records = startup_tbl.all(
            formula="{Run Match} = TRUE()",
            sort=['-Created Time']
        )
        
        # Return all matching records (no cache filtering)
        new_records = []
        for record in records:
            fields = record.get('fields', {})
            run_match_value = fields.get('Run Match', False)
            
            if is_run_match_enabled(run_match_value):
                new_records.append(record)
        
        return new_records
        
    except Exception as e:
        print(f"ERROR: Error checking for pending matches: {e}")
        return []

def update_workflow_status(record_id, status, message=""):
    """Update the workflow status of a startup record (only if fields exist)."""
    try:
        # Note: Run Match is now reset immediately when processing starts
        # This function is kept for potential future status field updates
        print(f"STATUS: Workflow status: {status} - {message}")
        
    except Exception as e:
        print(f"WARNING: Could not update record: {e}")

def run_workflow_matching(startup_record):
    """Run the workflow_matching.py script for a specific startup."""
    try:
        startup_name = startup_record.get('fields', {}).get('Startup Name', 'Unknown Startup')
        record_id = startup_record['id']
        
        print(f"\nSTARTING: workflow matching for: {startup_name}")
        print(f"RECORD ID: {record_id}")
        
        # Update status to "Running"
        update_workflow_status(record_id, "Running", f"Workflow matching started for {startup_name}")
        
        # Get the directory where this script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        workflow_script = os.path.join(script_dir, "workflow_matching.py")
        
        if not os.path.exists(workflow_script):
            error_msg = f"workflow_matching.py not found at: {workflow_script}"
            print(f"ERROR: {error_msg}")
            update_workflow_status(record_id, "Error", error_msg)
            return False
        
        # Run the workflow matching script WITH the specific startup ID
        print(f"EXECUTING: python {workflow_script} --startup-id {record_id}")
        
        result = subprocess.run(
            [sys.executable, workflow_script, "--startup-id", record_id],
            cwd=script_dir,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',  # Replace problematic characters instead of crashing
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            print(f"SUCCESS: Workflow matching completed successfully for: {startup_name}")
            update_workflow_status(record_id, "Completed", "Workflow matching completed successfully")
            return True
        else:
            # Safe error message handling
            stderr_msg = result.stderr if result.stderr else "No error details available"
            error_msg = f"Workflow matching failed with return code {result.returncode}. Error: {stderr_msg[:200]}"
            print(f"ERROR: {error_msg}")
            update_workflow_status(record_id, "Failed", error_msg)
            return False
            
    except subprocess.TimeoutExpired:
        error_msg = "Workflow matching timed out after 1 hour"
        print(f"TIMEOUT: {error_msg}")
        update_workflow_status(record_id, "Failed", error_msg)
        return False
        
    except Exception as e:
        error_msg = f"Unexpected error running workflow matching: {e}"
        print(f"ERROR: {error_msg}")
        update_workflow_status(record_id, "Failed", error_msg)
        return False

def main():
    """Main monitoring loop."""
    # Process lock to prevent multiple instances
    lock_file = '/tmp/auto_workflow_trigger.lock'
    lock_fd = None
    
    try:
        # Try to acquire exclusive lock
        lock_fd = open(lock_file, 'w')
        fcntl.lockf(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print("LOCK: Successfully acquired process lock")
        
        # Register cleanup on exit
        def cleanup():
            if lock_fd:
                fcntl.lockf(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
                if os.path.exists(lock_file):
                    os.remove(lock_file)
            print("CLEANUP: Released process lock")
        
        atexit.register(cleanup)
        
    except IOError:
        print("ERROR: Another instance of auto_workflow_trigger.py is already running!")
        print("TIP: Check with: ps aux | grep auto_workflow_trigger")
        print("TIP: Kill with: pkill -f auto_workflow_trigger.py")
        sys.exit(1)
    
    print("STARTING: Auto Workflow Trigger Started")
    print("MONITORING: Startup Submissions for 'Run Match' = True...")
    print("CHECKING: every 30 seconds...\n")
    
    check_count = 0
    processed_cache = {}  # Track processed records with timestamps
    
    try:
        while True:
            check_count += 1
            
            # Clean up old cache entries (older than 5 minutes)
            current_time = time.time()
            processed_cache = {k: v for k, v in processed_cache.items() 
                             if current_time - v < 300}
            
            # Check for records that need processing
            pending_records = get_pending_matches()
            
            # Filter out recently processed records
            new_pending = []
            for record in pending_records:
                record_id = record.get('id')
                if record_id not in processed_cache:
                    new_pending.append(record)
                else:
                    startup_name = record.get('fields', {}).get('Startup Name', 'Unknown')
                    print(f"SKIP: {startup_name} was recently processed (cache hit)")
            
            pending_records = new_pending
            
            if pending_records:
                print(f"\nFOUND: {len(pending_records)} record(s) ready for workflow matching!")
                
                # CRITICAL: Update ALL Run Match flags to False BEFORE any processing
                print("UPDATING: Setting Run Match to False for all pending records...")
                for record in pending_records:
                    record_id = record.get('id')
                    startup_name = record.get('fields', {}).get('Startup Name', 'Unknown Startup')
                    
                    # Add to cache immediately
                    processed_cache[record_id] = time.time()
                    
                    # Update Run Match to False BEFORE ANY processing starts
                    try:
                        startup_tbl.update(record_id, {"Run Match": False}, typecast=True)
                        print(f"  ✓ Disabled Run Match for: {startup_name}")
                    except Exception as e:
                        print(f"  ✗ Could not update {startup_name}: {e}")
                        # Remove from pending if we can't update the flag
                        pending_records.remove(record)
                
                print("UPDATES COMPLETE: All Run Match flags disabled\n")
                time.sleep(2)  # Brief pause to ensure Airtable syncs
                
                # Now process each record
                for record in pending_records:
                    record_id = record.get('id')
                    startup_name = record.get('fields', {}).get('Startup Name', 'Unknown Startup')
                    print(f"\n" + "="*60)
                    print(f"PROCESSING: {startup_name}")
                    print("="*60)
                    
                    success = run_workflow_matching(record)
                    
                    if success:
                        print(f"SUCCESS: Successfully processed: {startup_name}")
                    else:
                        print(f"FAILED: Failed to process: {startup_name}")
                    
                    # Small delay between records
                    time.sleep(5)
                
                print(f"\nCOMPLETED: Batch processing completed! Processed {len(pending_records)} record(s)")
                
            else:
                # Show periodic status
                if check_count % 20 == 0:  # Every 10 minutes (20 * 30 seconds)
                    print(f"STATUS: [{datetime.now().strftime('%H:%M:%S')}] Still monitoring... (Check #{check_count})")
                else:
                    print(".", end="", flush=True)
            
            # Wait before next check
            time.sleep(30)  # Check every 30 seconds
            
    except KeyboardInterrupt:
        print(f"\n\nSTOPPED: Auto Workflow Trigger stopped by user")
        print(f"STATS: Total checks performed: {check_count}")
        print(f"STATS: Service stopped")
        
    except Exception as e:
        print(f"\nERROR: Fatal error in monitoring loop: {e}")

if __name__ == "__main__":
    main()