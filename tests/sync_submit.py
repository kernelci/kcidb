#!/usr/bin/env python3
"""
Test program for synchronous kcidb submission using existing submission files
"""

import os
import sys
import json
import time
import glob
import argparse
from pathlib import Path

# Add the parent directory to the path to import kcidb
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import kcidb


def load_submission_files(submission_dir, limit=None):
    """Load submission files from the submissions directory"""
    pattern = os.path.join(submission_dir, "submission-*.json")
    files = glob.glob(pattern)
    
    if limit:
        files = files[:limit]
    
    submissions = []
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                submissions.append(data)
                print(f"Loaded submission from {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue
    
    return submissions


def main():
    parser = argparse.ArgumentParser(description="Test synchronous kcidb submission")
    parser.add_argument("--limit", "-l", type=int, help="Limit number of submissions to test", 
                       default=30)
    parser.add_argument("--submissions-dir", "-s", help="Directory containing submission files",
                       default="tests/submissions")
    parser.add_argument("--max-workers", "-w", type=int, help="Maximum number of worker threads", 
                       default=10)
    
    args = parser.parse_args()
    
    rest_uri = os.environ.get("KCIDB_REST")
    if not rest_uri:
        print("Error: KCIDB_REST environment variable is required")
        print("Example: export KCIDB_REST=https://token@db.kernelci.org/")
        return 1
    print(f"Using REST API: {rest_uri}")
    
    # Get the path to submissions directory
    script_dir = Path(__file__).parent.parent
    submissions_dir = script_dir / args.submissions_dir
    
    if not submissions_dir.exists():
        print(f"Error: Submissions directory not found: {submissions_dir}")
        return 1
    
    print(f"Loading submission files from: {submissions_dir}")
    submissions = load_submission_files(str(submissions_dir), args.limit)
    
    if not submissions:
        print("No submissions loaded, exiting")
        return 1
    
    print(f"Loaded {len(submissions)} submissions for testing")
    
    # Initialize kcidb client
    try:
        client = kcidb.Client(rest_uri=rest_uri, max_workers=args.max_workers)
        print(f"Initialized kcidb client for REST API with {args.max_workers} workers")
    except Exception as e:
        print(f"Error initializing kcidb client: {e}")
        return 1
    
    # Submit each submission synchronously
    print(f"\n--- Starting synchronous submission of {len(submissions)} files ---")
    successful_submissions = []
    failed_submissions = []
    
    start_time = time.time()
    
    for i, submission_data in enumerate(submissions, 1):
        print(f"\nSubmitting {i}/{len(submissions)}...")
        try:
            submission_id = client.submit(submission_data)
            print(f"OK: Submission {i} successful: {submission_id}")
            successful_submissions.append(submission_id)
        except Exception as e:
            print(f"ERR: Submission {i} failed: {e}")
            failed_submissions.append(str(e))
            # Continue with other submissions even if one fails
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Print summary
    print(f"\n--- Synchronous Submission Summary ---")
    print(f"Total submissions attempted: {len(submissions)}")
    print(f"Successful submissions: {len(successful_submissions)}")
    print(f"Failed submissions: {len(failed_submissions)}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average time per submission: {total_time/len(submissions):.2f} seconds")
    
    if successful_submissions:
        print(f"\nSuccessful submission IDs:")
        for sub_id in successful_submissions:
            print(f"  - {sub_id}")
    
    if failed_submissions:
        print(f"\nFailed submissions:")
        for error in failed_submissions:
            print(f"  - {error}")
    
    return 0 if not failed_submissions else 1


if __name__ == "__main__":
    sys.exit(main())
