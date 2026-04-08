#!/usr/bin/env python3
"""
Test program for asynchronous kcidb submission using futures with existing submission files
"""

import os
import sys
import json
import time
import glob
import argparse
import concurrent.futures
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
    parser = argparse.ArgumentParser(description="Test asynchronous kcidb submission using futures")
    parser.add_argument("--limit", "-l", type=int, help="Limit number of submissions to test", 
                       default=30)
    parser.add_argument("--submissions-dir", "-s", help="Directory containing submission files",
                       default="tests/submissions")
    parser.add_argument("--max-workers", "-w", type=int, help="Maximum number of worker threads", 
                       default=10)
    parser.add_argument("--timeout", help="Timeout for futures in seconds", type=int, default=30)
    
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
    
    # Submit all submissions asynchronously using futures
    print(f"\n--- Starting asynchronous submission of {len(submissions)} files ---")
    
    start_time = time.time()
    
    # Create futures for all submissions
    futures = []
    for i, submission_data in enumerate(submissions, 1):
        print(f"Creating future for submission {i}/{len(submissions)}...")
        try:
            future = client.future_submit(submission_data)
            futures.append((i, future))
        except Exception as e:
            print(f"ERR: Error creating future for submission {i}: {e}")
            futures.append((i, None))
    
    futures_creation_time = time.time()
    print(f"All futures created in {futures_creation_time - start_time:.2f} seconds")
    
    # Process results as they complete
    successful_submissions = []
    failed_submissions = []
    
    print(f"\nWaiting for submissions to complete (timeout: {args.timeout}s)...")
    
    for i, future in futures:
        if future is None:
            failed_submissions.append(f"Submission {i}: Future creation failed")
            continue
            
        try:
            # Wait for this specific future with timeout
            submission_id = future.result(timeout=args.timeout)
            print(f"OK: Submission {i} successful: {submission_id}")
            successful_submissions.append(submission_id)
        except concurrent.futures.TimeoutError:
            print(f"ERR: Submission {i} timed out after {args.timeout} seconds")
            failed_submissions.append(f"Submission {i}: Timeout")
        except Exception as e:
            print(f"ERR: Submission {i} failed: {e}")
            failed_submissions.append(f"Submission {i}: {e}")
    
    end_time = time.time()
    total_time = end_time - start_time
    processing_time = end_time - futures_creation_time
    
    # Print summary
    print(f"\n--- Asynchronous Submission Summary ---")
    print(f"Total submissions attempted: {len(submissions)}")
    print(f"Successful submissions: {len(successful_submissions)}")
    print(f"Failed submissions: {len(failed_submissions)}")
    print(f"Futures creation time: {futures_creation_time - start_time:.2f} seconds")
    print(f"Processing time: {processing_time:.2f} seconds")
    print(f"Total time: {total_time:.2f} seconds")
    
    if len(submissions) > 0:
        print(f"Average time per submission: {total_time/len(submissions):.2f} seconds")
    
    if successful_submissions:
        print(f"\nSuccessful submission IDs:")
        for sub_id in successful_submissions:
            print(f"  - {sub_id}")
    
    if failed_submissions:
        print(f"\nFailed submissions:")
        for error in failed_submissions:
            print(f"  - {error}")
    
    # Additional performance comparison
    print(f"\n--- Performance Benefits ---")
    print(f"Asynchronous approach allows {len(submissions)} submissions to be processed concurrently")
    print(f"instead of sequentially, potentially reducing total wait time.")
    
    return 0 if not failed_submissions else 1


if __name__ == "__main__":
    sys.exit(main())
