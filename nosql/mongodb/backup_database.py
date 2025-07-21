#!/usr/bin/env python3
"""
MongoDB Database Backup Script (Python)
Author: DBA Portfolio
Purpose: Comprehensive MongoDB backup with replica set and sharding support
"""

import os
import sys
import subprocess
import datetime
import argparse
import logging
import json
import gzip
import shutil
import glob
from pathlib import Path
import pymongo
from pymongo import MongoClient
from bson import json_util

class MongoDBBackup:
    def __init__(self, config):
        self.config = config
        self.setup_logging()
        self.client = None
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_file = os.path.join(self.config['backup_path'], 'mongodb_backup.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect_to_mongodb(self):
        """Establish connection to MongoDB"""
        try:
            # Build connection string
            if self.config.get('username') and self.config.get('password'):
                connection_string = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config.get('auth_database', 'admin')}"
            else:
                connection_string = f"mongodb://{self.config['host']}:{self.config['port']}"
            
            # Add replica set if specified
            if self.config.get('replica_set'):
                connection_string += f"?replicaSet={self.config['replica_set']}"
            
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            
            # Test connection
            self.client.admin.command('ping')
            self.logger.info(f"Connected to MongoDB at {self.config['host']}:{self.config['port']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {str(e)}")
            return False
    
    def get_database_info(self):
        """Get information about databases and collections"""
        try:
            db_stats = {}
            
            # Get list of databases
            databases = self.client.list_database_names()
            
            for db_name in databases:
                if db_name in ['admin', 'config', 'local'] and not self.config.get('include_system_dbs', False):
                    continue
                    
                db = self.client[db_name]
                collections = db.list_collection_names()
                
                db_stats[db_name] = {
                    'collections': len(collections),
                    'collection_names': collections,
                    'stats': db.command('dbStats')
                }
            
            self.logger.info(f"Found {len(db_stats)} user databases")
            return db_stats
            
        except Exception as e:
            self.logger.error(f"Failed to get database info: {str(e)}")
            return {}
    
    def create_backup_directory(self):
        """Create backup directory structure"""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.join(self.config['backup_path'], f"mongodb_backup_{timestamp}")
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        return backup_dir
    
    def backup_using_mongodump(self, backup_dir):
        """Perform backup using mongodump"""
        try:
            # Build mongodump command
            cmd = [self.config.get('mongodump_path', 'mongodump')]
            
            # Connection parameters
            cmd.extend(['--host', f"{self.config['host']}:{self.config['port']}"])
            
            if self.config.get('username'):
                cmd.extend(['--username', self.config['username']])
                cmd.extend(['--password', self.config['password']])
                cmd.extend(['--authenticationDatabase', self.config.get('auth_database', 'admin')])
            
            # Output directory
            cmd.extend(['--out', backup_dir])
            
            # Specific database if specified
            if self.config.get('database'):
                cmd.extend(['--db', self.config['database']])
            
            # Additional options
            if self.config.get('gzip', True):
                cmd.append('--gzip')
            
            if self.config.get('oplog', False):
                cmd.append('--oplog')
            
            # Execute mongodump
            self.logger.info(f"Starting mongodump with command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info("mongodump completed successfully")
                return True
            else:
                self.logger.error(f"mongodump failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Backup failed: {str(e)}")
            return False
    
    def backup_with_custom_script(self, backup_dir):
        """Custom backup using pymongo (for specific collections or custom logic)"""
        try:
            if not self.config.get('database'):
                self.logger.error("Database name required for custom backup")
                return False
            
            db = self.client[self.config['database']]
            db_backup_dir = os.path.join(backup_dir, self.config['database'])
            Path(db_backup_dir).mkdir(exist_ok=True)
            
            collections = db.list_collection_names()
            
            for collection_name in collections:
                self.logger.info(f"Backing up collection: {collection_name}")
                collection = db[collection_name]
                
                # Export collection data
                collection_file = os.path.join(db_backup_dir, f"{collection_name}.json")
                with open(collection_file, 'w') as f:
                    for document in collection.find():
                        f.write(json_util.dumps(document) + '\n')
                
                # Export indexes
                indexes = list(collection.list_indexes())
                if indexes:
                    index_file = os.path.join(db_backup_dir, f"{collection_name}_indexes.json")
                    with open(index_file, 'w') as f:
                        json.dump(indexes, f, default=json_util.default, indent=2)
                
                # Get collection stats
                try:
                    stats = db.command('collStats', collection_name)
                    stats_file = os.path.join(db_backup_dir, f"{collection_name}_stats.json")
                    with open(stats_file, 'w') as f:
                        json.dump(stats, f, default=json_util.default, indent=2)
                except:
                    pass  # Stats not available for all collection types
            
            self.logger.info("Custom backup completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Custom backup failed: {str(e)}")
            return False
    
    def compress_backup(self, backup_dir):
        """Compress backup directory"""
        if not self.config.get('compress_archive', True):
            return backup_dir
            
        try:
            compressed_file = f"{backup_dir}.tar.gz"
            
            # Create compressed archive
            shutil.make_archive(backup_dir, 'gztar', backup_dir)
            
            # Remove original directory
            shutil.rmtree(backup_dir)
            
            self.logger.info(f"Backup compressed: {compressed_file}")
            return compressed_file
            
        except Exception as e:
            self.logger.error(f"Compression failed: {str(e)}")
            return backup_dir
    
    def verify_backup(self, backup_path):
        """Verify backup integrity"""
        try:
            if backup_path.endswith('.tar.gz'):
                # Verify compressed archive
                import tarfile
                with tarfile.open(backup_path, 'r:gz') as tar:
                    tar.getnames()  # This will raise exception if corrupted
                return True
            else:
                # Verify directory structure
                if not os.path.exists(backup_path):
                    return False
                
                # Check if backup contains data
                for root, dirs, files in os.walk(backup_path):
                    if files:  # Found at least one file
                        return True
                
                return False
                
        except Exception as e:
            self.logger.error(f"Backup verification failed: {str(e)}")
            return False
    
    def cleanup_old_backups(self):
        """Remove old backups based on retention policy"""
        try:
            pattern = os.path.join(self.config['backup_path'], "mongodb_backup_*")
            backup_items = glob.glob(pattern)
            
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=self.config['retention_days'])
            
            for backup_item in backup_items:
                item_time = datetime.datetime.fromtimestamp(os.path.getmtime(backup_item))
                if item_time < cutoff_date:
                    if os.path.isdir(backup_item):
                        shutil.rmtree(backup_item)
                    else:
                        os.remove(backup_item)
                    
                    self.logger.info(f"Removed old backup: {os.path.basename(backup_item)}")
                    
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")
    
    def get_replica_set_status(self):
        """Get replica set status if applicable"""
        try:
            status = self.client.admin.command('replSetGetStatus')
            self.logger.info(f"Replica set: {status.get('set', 'N/A')}")
            
            # Log member status
            for member in status.get('members', []):
                self.logger.info(f"Member {member['name']}: {member['stateStr']}")
            
            return status
        except Exception as e:
            self.logger.info("Not running in replica set mode")
            return None
    
    def get_sharding_status(self):
        """Get sharding status if applicable"""
        try:
            status = self.client.admin.command('listShards')
            self.logger.info(f"Sharded cluster with {len(status['shards'])} shards")
            
            for shard in status['shards']:
                self.logger.info(f"Shard: {shard['_id']} - {shard['host']}")
            
            return status
        except Exception as e:
            self.logger.info("Not running in sharded mode")
            return None
    
    def generate_backup_manifest(self, backup_path, db_info):
        """Generate backup manifest file"""
        manifest = {
            'backup_timestamp': datetime.datetime.now().isoformat(),
            'mongodb_version': self.client.server_info()['version'],
            'backup_type': self.config.get('backup_type', 'mongodump'),
            'databases': db_info,
            'backup_path': backup_path,
            'config': {k: v for k, v in self.config.items() if k != 'password'}  # Exclude password
        }
        
        # Add replica set info if available
        rs_status = self.get_replica_set_status()
        if rs_status:
            manifest['replica_set'] = rs_status.get('set')
        
        # Add sharding info if available  
        shard_status = self.get_sharding_status()
        if shard_status:
            manifest['sharding'] = True
            manifest['shard_count'] = len(shard_status['shards'])
        
        # Write manifest file
        if backup_path.endswith('.tar.gz'):
            manifest_file = backup_path.replace('.tar.gz', '_manifest.json')
        else:
            manifest_file = os.path.join(os.path.dirname(backup_path), 'backup_manifest.json')
        
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2, default=str)
        
        self.logger.info(f"Backup manifest created: {manifest_file}")
    
    def run(self):
        """Main execution method"""
        # Create backup directory
        Path(self.config['backup_path']).mkdir(parents=True, exist_ok=True)
        
        # Connect to MongoDB
        if not self.connect_to_mongodb():
            return False
        
        # Get database information
        db_info = self.get_database_info()
        if not db_info:
            self.logger.error("No databases found to backup")
            return False
        
        # Log database information
        for db_name, info in db_info.items():
            size_mb = info['stats'].get('dataSize', 0) / (1024 * 1024)
            self.logger.info(f"Database: {db_name}, Collections: {info['collections']}, Size: {size_mb:.2f} MB")
        
        # Create backup directory
        backup_dir = self.create_backup_directory()
        self.logger.info(f"Backup directory: {backup_dir}")
        
        # Perform backup
        backup_method = self.config.get('backup_method', 'mongodump')
        if backup_method == 'mongodump':
            success = self.backup_using_mongodump(backup_dir)
        else:
            success = self.backup_with_custom_script(backup_dir)
        
        if not success:
            return False
        
        # Compress backup if enabled
        final_backup_path = self.compress_backup(backup_dir)
        
        # Verify backup
        if not self.verify_backup(final_backup_path):
            self.logger.error("Backup verification failed")
            return False
        
        # Generate manifest
        self.generate_backup_manifest(final_backup_path, db_info)
        
        # Cleanup old backups
        self.cleanup_old_backups()
        
        # Log success metrics
        if os.path.isfile(final_backup_path):
            backup_size = os.path.getsize(final_backup_path) / (1024 * 1024)
            self.logger.info(f"Backup completed successfully. Size: {backup_size:.2f} MB")
        else:
            self.logger.info("Backup completed successfully")
        
        return True

def main():
    parser = argparse.ArgumentParser(description='MongoDB Backup Script')
    parser.add_argument('--host', default='localhost', help='MongoDB host')
    parser.add_argument('--port', type=int, default=27017, help='MongoDB port')
    parser.add_argument('--database', help='Specific database to backup (optional)')
    parser.add_argument('--username', help='MongoDB username')
    parser.add_argument('--password', help='MongoDB password')
    parser.add_argument('--auth-database', default='admin', help='Authentication database')
    parser.add_argument('--replica-set', help='Replica set name')
    parser.add_argument('--backup-path', default='/var/backups/mongodb', help='Backup directory')
    parser.add_argument('--mongodump-path', default='mongodump', help='Path to mongodump executable')
    parser.add_argument('--backup-method', choices=['mongodump', 'custom'], default='mongodump', help='Backup method')
    parser.add_argument('--retention-days', type=int, default=7, help='Backup retention in days')
    parser.add_argument('--compress-archive', action='store_true', default=True, help='Compress backup archive')
    parser.add_argument('--gzip', action='store_true', default=True, help='Use gzip compression in mongodump')
    parser.add_argument('--oplog', action='store_true', help='Include oplog in backup')
    parser.add_argument('--include-system-dbs', action='store_true', help='Include system databases')
    
    args = parser.parse_args()
    
    config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'username': args.username,
        'password': args.password,
        'auth_database': args.auth_database,
        'replica_set': args.replica_set,
        'backup_path': args.backup_path,
        'mongodump_path': args.mongodump_path,
        'backup_method': args.backup_method,
        'retention_days': args.retention_days,
        'compress_archive': args.compress_archive,
        'gzip': args.gzip,
        'oplog': args.oplog,
        'include_system_dbs': args.include_system_dbs
    }
    
    backup = MongoDBBackup(config)
    success = backup.run()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()