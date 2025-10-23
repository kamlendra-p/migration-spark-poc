#!/usr/bin/env python3
"""
Force cleanup of Spark processes and temporary files
Useful when Spark jobs hang or leave zombie processes
"""
import os
import subprocess
import sys
import signal

def cleanup_spark_processes():
    """Kill any running Spark processes"""
    print("🧹 Cleaning up Spark processes...")
    
    try:
        # Find and kill Spark driver processes
        result = subprocess.run(['pgrep', '-f', 'SparkSubmit'], capture_output=True, text=True)
        if result.stdout:
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                if pid:
                    print(f"Killing Spark process PID: {pid}")
                    os.kill(int(pid), signal.SIGTERM)
        else:
            print("No Spark driver processes found")
            
        # Find and kill Java processes related to Spark
        result = subprocess.run(['pgrep', '-f', 'java.*spark'], capture_output=True, text=True)
        if result.stdout:
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                if pid:
                    print(f"Killing Java/Spark process PID: {pid}")
                    os.kill(int(pid), signal.SIGTERM)
        else:
            print("No Java/Spark processes found")
            
    except Exception as e:
        print(f"Error cleaning processes: {e}")

def cleanup_spark_temp_files():
    """Remove Spark temporary files and directories"""
    print("🗂️  Cleaning up temporary files...")
    
    temp_dirs = [
        '/tmp/spark-*',
        '/tmp/hsperfdata_*',
        os.path.expanduser('~/spark-warehouse'),
        os.path.expanduser('~/.ivy2/cache'),
    ]
    
    for temp_pattern in temp_dirs:
        try:
            if '*' in temp_pattern:
                # Use shell glob expansion
                subprocess.run(f'rm -rf {temp_pattern}', shell=True, check=False)
            else:
                if os.path.exists(temp_pattern):
                    subprocess.run(['rm', '-rf', temp_pattern], check=False)
                    print(f"Removed: {temp_pattern}")
        except Exception as e:
            print(f"Error removing {temp_pattern}: {e}")

def stop_spark_context():
    """Try to stop any active Spark contexts programmatically"""
    print("🛑 Stopping Spark contexts...")
    
    try:
        from pyspark.sql import SparkSession
        
        # Get all active Spark sessions and stop them
        if hasattr(SparkSession, '_instantiatedSession') and SparkSession._instantiatedSession:
            spark = SparkSession._instantiatedSession
            spark.stop()
            print("Stopped active Spark session")
        else:
            print("No active Spark sessions found")
            
    except ImportError:
        print("PySpark not available, skipping programmatic cleanup")
    except Exception as e:
        print(f"Error stopping Spark context: {e}")

def check_ports():
    """Check if Spark UI ports are still occupied"""
    print("🔍 Checking Spark ports...")
    
    spark_ports = [4040, 4041, 4042, 4043, 4044]
    
    for port in spark_ports:
        try:
            result = subprocess.run(['lsof', '-i', f':{port}'], capture_output=True, text=True)
            if result.stdout:
                print(f"Port {port} is still occupied:")
                print(result.stdout)
            else:
                print(f"Port {port} is free")
        except Exception as e:
            print(f"Error checking port {port}: {e}")

def main():
    print("=" * 50)
    print("🚨 FORCE SPARK CLEANUP UTILITY")
    print("=" * 50)
    print("⚠️  WARNING: This will forcefully terminate Spark processes!")
    print("Only use this if Spark jobs are stuck or hanging.")
    print()
    
    response = input("Continue with cleanup? (y/N): ")
    if response.lower() != 'y':
        print("Cleanup cancelled.")
        sys.exit(0)
    
    print("\nStarting cleanup process...")
    
    # Step 1: Try graceful shutdown first
    stop_spark_context()
    
    # Step 2: Force kill processes
    cleanup_spark_processes()
    
    # Step 3: Clean temporary files
    cleanup_spark_temp_files()
    
    # Step 4: Check ports
    check_ports()
    
    print("\n✅ Cleanup complete!")
    print("You can now restart your Spark applications.")

if __name__ == "__main__":
    main()