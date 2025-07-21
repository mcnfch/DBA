#!/usr/bin/env python3
"""
Apache Cassandra Backup Script (Python)
Author: DBA Portfolio
Purpose: Comprehensive Cassandra backup and snapshot management
"""

import os
import sys
import json
import time
import shutil
import argparse
import logging
import datetime
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

class CassandraBackup:
    def __init__(self, hosts: List[str], keyspace: Optional[str] = None, 
                 username: Optional[str] = None, password: Optional[str] = None,
                 port: int = 9042, datacenter: Optional[str] = None):
        self.hosts = hosts
        self.keyspace = keyspace
        self.username = username
        self.password = password
        self.port = port
        self.datacenter = datacenter
        self.setup_logging()
        self.connect()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'cassandra_backup_{datetime.datetime.now().strftime("%Y%m%d")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Connect to Cassandra cluster"""
        try:
            # Setup authentication if provided
            auth_provider = None
            if self.username and self.password:
                auth_provider = PlainTextAuthProvider(
                    username=self.username,
                    password=self.password
                )
            
            # Setup load balancing policy
            load_balancing_policy = None
            if self.datacenter:
                load_balancing_policy = DCAwareRoundRobinPolicy(
                    local_dc=self.datacenter
                )
            
            # Create cluster connection
            self.cluster = Cluster(
                self.hosts,
                port=self.port,
                auth_provider=auth_provider,
                load_balancing_policy=load_balancing_policy,
                connect_timeout=10,
                control_connection_timeout=10
            )
            
            self.session = self.cluster.connect()
            self.logger.info(f"Connected to Cassandra cluster: {self.hosts}")
            
            # Test connection
            result = self.session.execute("SELECT release_version FROM system.local")
            version = result.one().release_version
            self.logger.info(f"Cassandra version: {version}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Cassandra: {e}")
            sys.exit(1)
    
    def get_cluster_info(self) -> Dict:
        """Get cluster information"""
        try:
            cluster_info = {
                'hosts': [],
                'keyspaces': [],
                'datacenter_info': {},
                'cluster_name': 'Unknown'
            }
            
            # Get cluster name
            result = self.session.execute("SELECT cluster_name FROM system.local")
            cluster_info['cluster_name'] = result.one().cluster_name
            
            # Get all hosts
            result = self.session.execute("""
                SELECT peer, data_center, rack, release_version, tokens 
                FROM system.peers
            """)
            
            for row in result:
                host_info = {
                    'peer': str(row.peer) if row.peer else 'Unknown',
                    'data_center': row.data_center,
                    'rack': row.rack,
                    'version': row.release_version,
                    'tokens': len(row.tokens) if row.tokens else 0
                }
                cluster_info['hosts'].append(host_info)
                
                # Count hosts per datacenter
                dc = row.data_center
                if dc not in cluster_info['datacenter_info']:
                    cluster_info['datacenter_info'][dc] = 0
                cluster_info['datacenter_info'][dc] += 1
            
            # Add local host info
            result = self.session.execute("""
                SELECT data_center, rack, release_version, tokens 
                FROM system.local
            """)
            local = result.one()
            cluster_info['hosts'].append({
                'peer': 'local',
                'data_center': local.data_center,
                'rack': local.rack,
                'version': local.release_version,
                'tokens': len(local.tokens) if local.tokens else 0
            })
            
            # Count local datacenter
            if local.data_center not in cluster_info['datacenter_info']:
                cluster_info['datacenter_info'][local.data_center] = 0
            cluster_info['datacenter_info'][local.data_center] += 1
            
            # Get keyspaces
            result = self.session.execute("""
                SELECT keyspace_name, strategy_class, strategy_options 
                FROM system_schema.keyspaces
            """)
            
            for row in result:
                if not row.keyspace_name.startswith('system'):
                    keyspace_info = {
                        'name': row.keyspace_name,
                        'strategy_class': row.strategy_class,
                        'replication': row.strategy_options
                    }
                    cluster_info['keyspaces'].append(keyspace_info)
            
            return cluster_info
            
        except Exception as e:
            self.logger.error(f"Failed to get cluster info: {e}")
            return {}
    
    def get_keyspace_tables(self, keyspace: str) -> List[Dict]:
        """Get tables in keyspace"""
        try:
            result = self.session.execute(f"""
                SELECT table_name, bloom_filter_fp_chance, caching, compaction, compression
                FROM system_schema.tables 
                WHERE keyspace_name = '{keyspace}'
            """)
            
            tables = []
            for row in result:
                table_info = {
                    'name': row.table_name,
                    'bloom_filter_fp_chance': row.bloom_filter_fp_chance,
                    'caching': row.caching,
                    'compaction': row.compaction,
                    'compression': row.compression
                }
                tables.append(table_info)
            
            return tables
            
        except Exception as e:
            self.logger.error(f"Failed to get tables for keyspace {keyspace}: {e}")
            return []
    
    def create_snapshot(self, snapshot_name: str, keyspace: Optional[str] = None, 
                       table: Optional[str] = None) -> Dict:
        """Create snapshot using nodetool"""
        try:
            self.logger.info(f"Creating snapshot: {snapshot_name}")
            
            # Build nodetool command
            cmd = ['nodetool', 'snapshot']
            
            if keyspace and table:
                cmd.extend(['-t', snapshot_name, keyspace, table])
                self.logger.info(f"Creating snapshot for table {keyspace}.{table}")
            elif keyspace:
                cmd.extend(['-t', snapshot_name, keyspace])
                self.logger.info(f"Creating snapshot for keyspace {keyspace}")
            else:
                cmd.extend(['-t', snapshot_name])
                self.logger.info("Creating snapshot for entire cluster")
            
            # Execute command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                self.logger.info(f"Snapshot created successfully: {snapshot_name}")
                
                # Get snapshot info
                snapshot_info = self.get_snapshot_info(snapshot_name)
                return {
                    'status': 'success',
                    'snapshot_name': snapshot_name,
                    'created_at': datetime.datetime.now().isoformat(),
                    'info': snapshot_info
                }
            else:
                self.logger.error(f"Snapshot creation failed: {result.stderr}")
                return {
                    'status': 'failed',
                    'error': result.stderr,
                    'snapshot_name': snapshot_name
                }
                
        except subprocess.TimeoutExpired:
            self.logger.error("Snapshot creation timed out")
            return {'status': 'timeout', 'snapshot_name': snapshot_name}
        except Exception as e:
            self.logger.error(f"Snapshot creation failed: {e}")
            return {'status': 'error', 'error': str(e), 'snapshot_name': snapshot_name}
    
    def get_snapshot_info(self, snapshot_name: Optional[str] = None) -> Dict:
        """Get snapshot information"""
        try:
            cmd = ['nodetool', 'listsnapshots']
            if snapshot_name:
                cmd.extend(['-t', snapshot_name])
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                # Parse nodetool output
                lines = result.stdout.strip().split('\n')
                snapshots = {}
                
                for line in lines[1:]:  # Skip header
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 5:
                            snap_name = parts[0]
                            keyspace = parts[1]
                            table = parts[2]
                            size = parts[3]
                            true_size = parts[4]
                            
                            if snap_name not in snapshots:
                                snapshots[snap_name] = {
                                    'name': snap_name,
                                    'tables': [],
                                    'total_size': 0,
                                    'total_true_size': 0
                                }
                            
                            snapshots[snap_name]['tables'].append({
                                'keyspace': keyspace,
                                'table': table,
                                'size': size,
                                'true_size': true_size
                            })
                
                return snapshots
            else:
                self.logger.error(f"Failed to get snapshot info: {result.stderr}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Failed to get snapshot info: {e}")
            return {}
    
    def clear_snapshot(self, snapshot_name: str, keyspace: Optional[str] = None) -> bool:
        """Clear/delete snapshot"""
        try:
            self.logger.info(f"Clearing snapshot: {snapshot_name}")
            
            cmd = ['nodetool', 'clearsnapshot']
            if keyspace:
                cmd.extend(['-t', snapshot_name, keyspace])
            else:
                cmd.extend(['-t', snapshot_name])
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                self.logger.info(f"Snapshot cleared successfully: {snapshot_name}")
                return True
            else:
                self.logger.error(f"Failed to clear snapshot: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to clear snapshot: {e}")
            return False
    
    def backup_schema(self, output_path: str, keyspace: Optional[str] = None) -> bool:
        """Backup schema using cqlsh"""
        try:
            self.logger.info(f"Backing up schema to: {output_path}")
            
            # Ensure output directory exists
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Build cqlsh command for schema export
            cmd = ['cqlsh']
            
            # Add host and port
            if self.hosts:
                cmd.extend(['-H', self.hosts[0], '-P', str(self.port)])
            
            # Add authentication if available
            if self.username:
                cmd.extend(['-u', self.username])
            if self.password:
                cmd.extend(['-p', self.password])
            
            # Add describe command
            if keyspace:
                cmd.extend(['-e', f"DESCRIBE KEYSPACE {keyspace};"])
            else:
                cmd.extend(['-e', 'DESCRIBE SCHEMA;'])
            
            # Execute and save output
            with open(output_path, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                      text=True, timeout=300)
            
            if result.returncode == 0:
                schema_size = os.path.getsize(output_path)
                self.logger.info(f"Schema backup completed: {self._format_bytes(schema_size)}")
                return True
            else:
                self.logger.error(f"Schema backup failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Schema backup failed: {e}")
            return False
    
    def copy_snapshot_data(self, snapshot_name: str, destination_dir: str,
                          keyspace: Optional[str] = None) -> bool:
        """Copy snapshot data to backup location"""
        try:
            self.logger.info(f"Copying snapshot data: {snapshot_name}")
            
            # Create destination directory
            Path(destination_dir).mkdir(parents=True, exist_ok=True)
            
            # Find Cassandra data directory (common locations)
            data_dirs = [
                '/var/lib/cassandra/data',
                '/opt/cassandra/data',
                '/data/cassandra/data',
                os.path.expanduser('~/cassandra/data')
            ]
            
            cassandra_data_dir = None
            for data_dir in data_dirs:
                if os.path.exists(data_dir):
                    cassandra_data_dir = data_dir
                    break
            
            if not cassandra_data_dir:
                self.logger.error("Could not find Cassandra data directory")
                return False
            
            self.logger.info(f"Using Cassandra data directory: {cassandra_data_dir}")
            
            # Copy snapshot files
            copied_files = 0
            total_size = 0
            
            # Walk through data directory looking for snapshots
            for root, dirs, files in os.walk(cassandra_data_dir):
                if 'snapshots' in root and snapshot_name in root:
                    # This is a snapshot directory
                    rel_path = os.path.relpath(root, cassandra_data_dir)
                    dest_path = os.path.join(destination_dir, rel_path)
                    
                    # Create destination directory structure
                    Path(dest_path).mkdir(parents=True, exist_ok=True)
                    
                    # Copy files
                    for file in files:
                        src_file = os.path.join(root, file)
                        dest_file = os.path.join(dest_path, file)
                        
                        # Filter by keyspace if specified
                        if keyspace and keyspace not in root:
                            continue
                        
                        try:
                            shutil.copy2(src_file, dest_file)
                            file_size = os.path.getsize(dest_file)
                            total_size += file_size
                            copied_files += 1
                            
                        except Exception as e:
                            self.logger.error(f"Failed to copy {src_file}: {e}")
            
            if copied_files > 0:
                self.logger.info(f"Copied {copied_files} files ({self._format_bytes(total_size)})")
                return True
            else:
                self.logger.warning("No snapshot files found to copy")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to copy snapshot data: {e}")
            return False
    
    def create_full_backup(self, backup_dir: str, backup_name: Optional[str] = None) -> Dict:
        """Create full backup including schema and data snapshots"""
        try:
            if not backup_name:
                backup_name = f"backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            backup_path = os.path.join(backup_dir, backup_name)
            Path(backup_path).mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"Creating full backup: {backup_name}")
            
            backup_result = {
                'backup_name': backup_name,
                'backup_path': backup_path,
                'started_at': datetime.datetime.now().isoformat(),
                'schema_backup': False,
                'snapshot_created': False,
                'data_copied': False,
                'cluster_info': {},
                'errors': []
            }
            
            # Get cluster information
            backup_result['cluster_info'] = self.get_cluster_info()
            
            # Backup schema
            schema_file = os.path.join(backup_path, 'schema.cql')
            if self.backup_schema(schema_file, self.keyspace):
                backup_result['schema_backup'] = True
            else:
                backup_result['errors'].append('Schema backup failed')
            
            # Create snapshot
            snapshot_name = f"backup_snap_{int(time.time())}"
            snapshot_result = self.create_snapshot(snapshot_name, self.keyspace)
            
            if snapshot_result['status'] == 'success':
                backup_result['snapshot_created'] = True
                backup_result['snapshot_name'] = snapshot_name
                
                # Copy snapshot data
                data_dir = os.path.join(backup_path, 'data')
                if self.copy_snapshot_data(snapshot_name, data_dir, self.keyspace):
                    backup_result['data_copied'] = True
                else:
                    backup_result['errors'].append('Data copy failed')
                
                # Clean up snapshot after copying
                if self.clear_snapshot(snapshot_name, self.keyspace):
                    self.logger.info("Temporary snapshot cleaned up")
                
            else:
                backup_result['errors'].append(f"Snapshot creation failed: {snapshot_result.get('error', 'Unknown error')}")
            
            # Create backup manifest
            manifest_file = os.path.join(backup_path, 'manifest.json')
            backup_result['completed_at'] = datetime.datetime.now().isoformat()
            
            with open(manifest_file, 'w') as f:
                json.dump(backup_result, f, indent=2)
            
            # Determine overall status
            if backup_result['schema_backup'] and backup_result['data_copied']:
                backup_result['status'] = 'success'
                self.logger.info(f"Full backup completed successfully: {backup_name}")
            else:
                backup_result['status'] = 'partial'
                self.logger.warning(f"Backup completed with errors: {backup_result['errors']}")
            
            return backup_result
            
        except Exception as e:
            self.logger.error(f"Full backup failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'backup_name': backup_name if 'backup_name' in locals() else 'Unknown'
            }
    
    def list_backups(self, backup_dir: str) -> List[Dict]:
        """List available backups"""
        try:
            backups = []
            
            if not os.path.exists(backup_dir):
                return backups
            
            for item in os.listdir(backup_dir):
                item_path = os.path.join(backup_dir, item)
                if os.path.isdir(item_path):
                    manifest_file = os.path.join(item_path, 'manifest.json')
                    
                    if os.path.exists(manifest_file):
                        try:
                            with open(manifest_file, 'r') as f:
                                manifest = json.load(f)
                            
                            backup_info = {
                                'name': item,
                                'path': item_path,
                                'status': manifest.get('status', 'Unknown'),
                                'created_at': manifest.get('started_at', 'Unknown'),
                                'size': self._get_directory_size(item_path),
                                'has_schema': os.path.exists(os.path.join(item_path, 'schema.cql')),
                                'has_data': os.path.exists(os.path.join(item_path, 'data'))
                            }
                            
                            backups.append(backup_info)
                            
                        except json.JSONDecodeError:
                            self.logger.warning(f"Invalid manifest file: {manifest_file}")
                    else:
                        # Directory without manifest - might be an old backup
                        backup_info = {
                            'name': item,
                            'path': item_path,
                            'status': 'Unknown',
                            'created_at': 'Unknown',
                            'size': self._get_directory_size(item_path),
                            'has_schema': os.path.exists(os.path.join(item_path, 'schema.cql')),
                            'has_data': os.path.exists(os.path.join(item_path, 'data'))
                        }
                        backups.append(backup_info)
            
            # Sort by creation time
            backups.sort(key=lambda x: x['created_at'], reverse=True)
            return backups
            
        except Exception as e:
            self.logger.error(f"Failed to list backups: {e}")
            return []
    
    def cleanup_old_backups(self, backup_dir: str, retention_days: int) -> int:
        """Clean up old backups based on retention policy"""
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
            deleted_count = 0
            
            backups = self.list_backups(backup_dir)
            
            for backup in backups:
                try:
                    # Parse creation date
                    if backup['created_at'] != 'Unknown':
                        backup_date = datetime.datetime.fromisoformat(backup['created_at'].replace('Z', '+00:00'))
                        if backup_date.replace(tzinfo=None) < cutoff_date:
                            # Delete old backup
                            shutil.rmtree(backup['path'])
                            self.logger.info(f"Deleted old backup: {backup['name']}")
                            deleted_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to delete backup {backup['name']}: {e}")
            
            self.logger.info(f"Cleaned up {deleted_count} old backups")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Backup cleanup failed: {e}")
            return 0
    
    def _get_directory_size(self, path: str) -> str:
        """Get directory size in human readable format"""
        try:
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
            return self._format_bytes(total_size)
        except:
            return "Unknown"
    
    def _format_bytes(self, bytes_size: int) -> str:
        """Format byte size to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} PB"
    
    def close(self):
        """Close Cassandra connection"""
        if hasattr(self, 'cluster') and self.cluster:
            self.cluster.shutdown()

def main():
    parser = argparse.ArgumentParser(description='Cassandra Backup Management Tool')
    parser.add_argument('--hosts', nargs='+', default=['localhost'], 
                       help='Cassandra host addresses')
    parser.add_argument('--port', type=int, default=9042, help='Cassandra port')
    parser.add_argument('--keyspace', help='Specific keyspace to backup')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--datacenter', help='Local datacenter name')
    parser.add_argument('--backup-dir', default='/var/backups/cassandra', 
                       help='Backup directory')
    parser.add_argument('--action', choices=['backup', 'list', 'cleanup', 'snapshot', 'info'], 
                       default='backup', help='Action to perform')
    parser.add_argument('--retention-days', type=int, default=7, 
                       help='Backup retention in days')
    parser.add_argument('--backup-name', help='Custom backup name')
    parser.add_argument('--snapshot-name', help='Snapshot name for snapshot operations')
    
    args = parser.parse_args()
    
    # Initialize backup manager
    backup_manager = CassandraBackup(
        hosts=args.hosts,
        keyspace=args.keyspace,
        username=args.username,
        password=args.password,
        port=args.port,
        datacenter=args.datacenter
    )
    
    try:
        if args.action == 'backup':
            result = backup_manager.create_full_backup(args.backup_dir, args.backup_name)
            print(f"Backup Status: {result.get('status', 'Unknown')}")
            if result.get('errors'):
                print(f"Errors: {result['errors']}")
        
        elif args.action == 'list':
            backups = backup_manager.list_backups(args.backup_dir)
            print(f"\nFound {len(backups)} backups:")
            print("-" * 80)
            for backup in backups:
                print(f"Name: {backup['name']}")
                print(f"  Status: {backup['status']}")
                print(f"  Created: {backup['created_at']}")
                print(f"  Size: {backup['size']}")
                print(f"  Schema: {'Yes' if backup['has_schema'] else 'No'}")
                print(f"  Data: {'Yes' if backup['has_data'] else 'No'}")
                print()
        
        elif args.action == 'cleanup':
            deleted = backup_manager.cleanup_old_backups(args.backup_dir, args.retention_days)
            print(f"Cleaned up {deleted} old backups")
        
        elif args.action == 'snapshot':
            if not args.snapshot_name:
                args.snapshot_name = f"snapshot_{int(time.time())}"
            
            result = backup_manager.create_snapshot(args.snapshot_name, args.keyspace)
            print(f"Snapshot Status: {result['status']}")
            if result['status'] != 'success':
                print(f"Error: {result.get('error', 'Unknown error')}")
        
        elif args.action == 'info':
            cluster_info = backup_manager.get_cluster_info()
            print("\nCluster Information:")
            print("=" * 50)
            print(f"Cluster Name: {cluster_info.get('cluster_name', 'Unknown')}")
            print(f"Total Hosts: {len(cluster_info.get('hosts', []))}")
            print(f"Datacenters: {list(cluster_info.get('datacenter_info', {}).keys())}")
            print(f"User Keyspaces: {len(cluster_info.get('keyspaces', []))}")
            
            print(f"\nKeyspaces:")
            for ks in cluster_info.get('keyspaces', []):
                print(f"  - {ks['name']} ({ks.get('strategy_class', 'Unknown strategy')})")
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        backup_manager.logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        backup_manager.close()

if __name__ == "__main__":
    main()