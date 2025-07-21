#!/usr/bin/env python3
"""
PostgreSQL Database Backup Script (Python)
Author: DBA Portfolio
Purpose: Cross-platform PostgreSQL backup automation with monitoring
"""

import os
import sys
import subprocess
import datetime
import argparse
import logging
import gzip
import shutil
import glob
import psycopg2
from pathlib import Path

class PostgreSQLBackup:
    def __init__(self, config):
        self.config = config
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = os.path.join(self.config['backup_path'], 'backup.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def test_connection(self):
        """Test database connection"""
        try:
            conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password']
            )
            conn.close()
            self.logger.info(f"Connection to {self.config['database']} successful")
            return True
        except Exception as e:
            self.logger.error(f"Connection failed: {str(e)}")
            return False
    
    def create_backup_directory(self):
        """Create backup directory if it doesn't exist"""
        Path(self.config['backup_path']).mkdir(parents=True, exist_ok=True)
    
    def get_backup_filename(self):
        """Generate backup filename with timestamp"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.config['database']}_backup_{timestamp}.sql"
    
    def perform_backup(self):
        """Perform the actual backup using pg_dump"""
        backup_file = os.path.join(self.config['backup_path'], self.get_backup_filename())
        
        # Prepare pg_dump command
        cmd = [
            self.config['pg_dump_path'],
            f"--host={self.config['host']}",
            f"--port={self.config['port']}",
            f"--username={self.config['username']}",
            f"--dbname={self.config['database']}",
            "--verbose",
            "--clean",
            "--create",
            "--if-exists",
            f"--file={backup_file}"
        ]
        
        # Set environment variables
        env = os.environ.copy()
        env['PGPASSWORD'] = self.config['password']
        
        try:
            self.logger.info(f"Starting backup of {self.config['database']}")
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"Backup completed: {backup_file}")
                
                # Compress backup if enabled
                if self.config.get('compress', True):
                    compressed_file = self.compress_backup(backup_file)
                    if compressed_file:
                        os.remove(backup_file)
                        backup_file = compressed_file
                
                # Verify backup
                if self.verify_backup(backup_file):
                    self.logger.info("Backup verification successful")
                    return backup_file
                else:
                    self.logger.error("Backup verification failed")
                    return None
                    
            else:
                self.logger.error(f"pg_dump failed: {result.stderr}")
                return None
                
        except Exception as e:
            self.logger.error(f"Backup failed: {str(e)}")
            return None
    
    def compress_backup(self, backup_file):
        """Compress backup file using gzip"""
        try:
            compressed_file = f"{backup_file}.gz"
            with open(backup_file, 'rb') as f_in:
                with gzip.open(compressed_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            self.logger.info(f"Backup compressed: {compressed_file}")
            return compressed_file
        except Exception as e:
            self.logger.error(f"Compression failed: {str(e)}")
            return None
    
    def verify_backup(self, backup_file):
        """Verify backup file integrity"""
        try:
            if backup_file.endswith('.gz'):
                with gzip.open(backup_file, 'rt') as f:
                    first_line = f.readline()
            else:
                with open(backup_file, 'r') as f:
                    first_line = f.readline()
            
            # Check if it looks like a valid SQL dump
            return 'PostgreSQL database dump' in first_line or 'CREATE DATABASE' in first_line
        except:
            return False
    
    def cleanup_old_backups(self):
        """Remove old backups based on retention policy"""
        try:
            pattern = os.path.join(self.config['backup_path'], f"{self.config['database']}_backup_*.sql*")
            backup_files = glob.glob(pattern)
            
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=self.config['retention_days'])
            
            for backup_file in backup_files:
                file_time = datetime.datetime.fromtimestamp(os.path.getmtime(backup_file))
                if file_time < cutoff_date:
                    os.remove(backup_file)
                    self.logger.info(f"Removed old backup: {backup_file}")
                    
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")
    
    def get_database_size(self):
        """Get database size for monitoring"""
        try:
            conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password']
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT pg_size_pretty(pg_database_size(%s))", (self.config['database'],))
            size = cursor.fetchone()[0]
            conn.close()
            
            return size
        except Exception as e:
            self.logger.error(f"Failed to get database size: {str(e)}")
            return "Unknown"
    
    def run(self):
        """Main execution method"""
        self.create_backup_directory()
        
        if not self.test_connection():
            return False
        
        db_size = self.get_database_size()
        self.logger.info(f"Database size: {db_size}")
        
        backup_file = self.perform_backup()
        if backup_file:
            self.cleanup_old_backups()
            
            # Log success metrics
            backup_size = os.path.getsize(backup_file)
            self.logger.info(f"Backup size: {backup_size / (1024*1024):.2f} MB")
            return True
        else:
            return False

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL Backup Script')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', default='5432', help='Database port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--username', required=True, help='Database username')
    parser.add_argument('--password', help='Database password (use PGPASSWORD env var instead)')
    parser.add_argument('--backup-path', default='/var/backups/postgresql', help='Backup directory')
    parser.add_argument('--pg-dump-path', default='pg_dump', help='Path to pg_dump executable')
    parser.add_argument('--retention-days', type=int, default=7, help='Backup retention in days')
    parser.add_argument('--compress', action='store_true', default=True, help='Compress backup files')
    
    args = parser.parse_args()
    
    # Get password from environment if not provided
    password = args.password or os.environ.get('PGPASSWORD')
    if not password:
        print("Error: Password must be provided via --password or PGPASSWORD environment variable")
        sys.exit(1)
    
    config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'username': args.username,
        'password': password,
        'backup_path': args.backup_path,
        'pg_dump_path': args.pg_dump_path,
        'retention_days': args.retention_days,
        'compress': args.compress
    }
    
    backup = PostgreSQLBackup(config)
    success = backup.run()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()