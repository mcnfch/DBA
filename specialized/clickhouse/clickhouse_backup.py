#!/usr/bin/env python3
"""
ClickHouse Database Backup Script (Python)
Author: DBA Portfolio
Purpose: Comprehensive ClickHouse backup and management for analytics workloads
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
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

class ClickHouseBackup:
    def __init__(self, host: str = 'localhost', port: int = 9000, 
                 database: str = 'default', username: str = 'default',
                 password: str = '', secure: bool = False):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.secure = secure
        self.setup_logging()
        self.connect()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'clickhouse_backup_{datetime.datetime.now().strftime("%Y%m%d")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Connect to ClickHouse server"""
        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                secure=self.secure,
                connect_timeout=10,
                send_receive_timeout=60
            )
            
            # Test connection
            result = self.client.execute('SELECT version()')
            version = result[0][0] if result else 'Unknown'
            self.logger.info(f"Connected to ClickHouse {version} at {self.host}:{self.port}")
            
        except ClickHouseError as e:
            self.logger.error(f"Failed to connect to ClickHouse: {e}")
            sys.exit(1)
    
    def get_server_info(self) -> Dict:
        """Get ClickHouse server information"""
        try:
            info = {}
            
            # Basic server info
            result = self.client.execute('SELECT version(), uptime()')
            if result:
                info['version'] = result[0][0]
                info['uptime_seconds'] = result[0][1]
                info['uptime_days'] = result[0][1] // 86400
            
            # Server settings
            settings_query = """
            SELECT name, value
            FROM system.settings
            WHERE name IN ('max_memory_usage', 'max_threads', 'max_execution_time')
            """
            settings_result = self.client.execute(settings_query)
            info['settings'] = {name: value for name, value in settings_result}
            
            # Disk usage
            try:
                disk_query = """
                SELECT
                    name,
                    path,
                    formatReadableSize(free_space) as free_space,
                    formatReadableSize(total_space) as total_space,
                    round(free_space / total_space * 100, 2) as free_percent
                FROM system.disks
                """
                disk_result = self.client.execute(disk_query)
                info['disks'] = [
                    {
                        'name': row[0],
                        'path': row[1], 
                        'free_space': row[2],
                        'total_space': row[3],
                        'free_percent': row[4]
                    }
                    for row in disk_result
                ]
            except:
                info['disks'] = []
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get server info: {e}")
            return {}
    
    def get_databases_info(self) -> List[Dict]:
        """Get information about databases"""
        try:
            query = """
            SELECT 
                database,
                engine,
                data_path,
                metadata_path
            FROM system.databases
            WHERE database NOT IN ('INFORMATION_SCHEMA', 'information_schema')
            ORDER BY database
            """
            
            result = self.client.execute(query)
            databases = []
            
            for row in result:
                db_info = {
                    'name': row[0],
                    'engine': row[1],
                    'data_path': row[2],
                    'metadata_path': row[3],
                    'tables': self.get_tables_info(row[0])
                }
                databases.append(db_info)
            
            return databases
            
        except Exception as e:
            self.logger.error(f"Failed to get databases info: {e}")
            return []
    
    def get_tables_info(self, database: str) -> List[Dict]:
        """Get information about tables in a database"""
        try:
            query = f"""
            SELECT 
                name,
                engine,
                total_rows,
                total_bytes,
                formatReadableSize(total_bytes) as size_human
            FROM system.tables
            WHERE database = '{database}'
            ORDER BY total_bytes DESC
            """
            
            result = self.client.execute(query)
            tables = []
            
            for row in result:
                table_info = {
                    'name': row[0],
                    'engine': row[1],
                    'rows': row[2],
                    'bytes': row[3],
                    'size_human': row[4]
                }
                tables.append(table_info)
            
            return tables
            
        except Exception as e:
            self.logger.error(f"Failed to get tables info for database {database}: {e}")
            return []
    
    def create_backup(self, backup_dir: str, databases: Optional[List[str]] = None,
                     backup_name: Optional[str] = None, include_data: bool = True) -> Dict:
        """Create backup of databases"""
        try:
            if not backup_name:
                backup_name = f"clickhouse_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            backup_path = os.path.join(backup_dir, backup_name)
            Path(backup_path).mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"Creating ClickHouse backup: {backup_name}")
            
            backup_result = {
                'backup_name': backup_name,
                'backup_path': backup_path,
                'started_at': datetime.datetime.now().isoformat(),
                'databases_backed_up': [],
                'schema_files': [],
                'data_files': [],
                'errors': [],
                'total_size': 0
            }
            
            # Get databases to backup
            if not databases:
                db_info = self.get_databases_info()
                databases = [db['name'] for db in db_info if db['name'] not in ['system', 'INFORMATION_SCHEMA']]
            
            for database in databases:
                try:
                    self.logger.info(f"Backing up database: {database}")
                    
                    # Create database directory
                    db_backup_dir = os.path.join(backup_path, database)
                    Path(db_backup_dir).mkdir(exist_ok=True)
                    
                    # Backup schema
                    schema_result = self.backup_database_schema(database, db_backup_dir)
                    if schema_result:
                        backup_result['schema_files'].extend(schema_result)
                    
                    # Backup data if requested
                    if include_data:
                        data_result = self.backup_database_data(database, db_backup_dir)
                        if data_result:
                            backup_result['data_files'].extend(data_result)
                    
                    backup_result['databases_backed_up'].append(database)
                    
                except Exception as e:
                    error_msg = f"Failed to backup database {database}: {str(e)}"
                    self.logger.error(error_msg)
                    backup_result['errors'].append(error_msg)
            
            # Calculate total backup size
            backup_result['total_size'] = self._get_directory_size(backup_path)
            backup_result['completed_at'] = datetime.datetime.now().isoformat()
            
            # Create backup manifest
            manifest_file = os.path.join(backup_path, 'manifest.json')
            with open(manifest_file, 'w') as f:
                json.dump(backup_result, f, indent=2, default=str)
            
            # Determine status
            if backup_result['errors']:
                backup_result['status'] = 'completed_with_errors'
                self.logger.warning(f"Backup completed with {len(backup_result['errors'])} errors")
            else:
                backup_result['status'] = 'success'
                self.logger.info(f"Backup completed successfully: {self._format_bytes(backup_result['total_size'])}")
            
            return backup_result
            
        except Exception as e:
            self.logger.error(f"Backup creation failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'backup_name': backup_name if 'backup_name' in locals() else 'Unknown'
            }
    
    def backup_database_schema(self, database: str, output_dir: str) -> List[str]:
        """Backup database schema (CREATE statements)"""
        try:
            schema_files = []
            
            # Get all tables in database
            tables = self.get_tables_info(database)
            
            for table in tables:
                table_name = table['name']
                
                try:
                    # Get CREATE statement
                    create_query = f"SHOW CREATE TABLE {database}.{table_name}"
                    result = self.client.execute(create_query)
                    
                    if result and result[0]:
                        create_statement = result[0][0]
                        
                        # Save to file
                        schema_file = os.path.join(output_dir, f"{table_name}_schema.sql")
                        with open(schema_file, 'w') as f:
                            f.write(f"-- ClickHouse table schema for {database}.{table_name}\n")
                            f.write(f"-- Generated: {datetime.datetime.now().isoformat()}\n\n")
                            f.write(create_statement)
                            f.write(";\n")
                        
                        schema_files.append(schema_file)
                        self.logger.debug(f"Schema saved for table: {table_name}")
                
                except Exception as e:
                    self.logger.error(f"Failed to backup schema for table {table_name}: {e}")
            
            return schema_files
            
        except Exception as e:
            self.logger.error(f"Schema backup failed for database {database}: {e}")
            return []
    
    def backup_database_data(self, database: str, output_dir: str) -> List[str]:
        """Backup database data using clickhouse-client"""
        try:
            data_files = []
            tables = self.get_tables_info(database)
            
            for table in tables:
                table_name = table['name']
                
                # Skip empty tables
                if table['rows'] == 0:
                    self.logger.debug(f"Skipping empty table: {table_name}")
                    continue
                
                try:
                    self.logger.info(f"Backing up data for table: {database}.{table_name} ({table['rows']:,} rows)")
                    
                    # Export data to CSV
                    data_file = os.path.join(output_dir, f"{table_name}_data.csv")
                    
                    # Use clickhouse-client for data export
                    export_cmd = [
                        'clickhouse-client',
                        f'--host={self.host}',
                        f'--port={self.port}',
                        f'--user={self.username}',
                        '--format=CSV',
                        f'--query=SELECT * FROM {database}.{table_name}'
                    ]
                    
                    if self.password:
                        export_cmd.append(f'--password={self.password}')
                    
                    # Execute export
                    with open(data_file, 'w') as f:
                        result = subprocess.run(export_cmd, stdout=f, stderr=subprocess.PIPE, text=True)
                    
                    if result.returncode == 0:
                        file_size = os.path.getsize(data_file)
                        if file_size > 0:
                            data_files.append(data_file)
                            self.logger.debug(f"Data exported: {table_name} ({self._format_bytes(file_size)})")
                        else:
                            os.remove(data_file)
                            self.logger.warning(f"Empty export for table: {table_name}")
                    else:
                        self.logger.error(f"Data export failed for {table_name}: {result.stderr}")
                        if os.path.exists(data_file):
                            os.remove(data_file)
                
                except Exception as e:
                    self.logger.error(f"Failed to backup data for table {table_name}: {e}")
            
            return data_files
            
        except Exception as e:
            self.logger.error(f"Data backup failed for database {database}: {e}")
            return []
    
    def restore_backup(self, backup_path: str, target_database: Optional[str] = None) -> bool:
        """Restore backup from backup directory"""
        try:
            self.logger.info(f"Restoring backup from: {backup_path}")
            
            # Read manifest
            manifest_file = os.path.join(backup_path, 'manifest.json')
            if not os.path.exists(manifest_file):
                self.logger.error("Backup manifest not found")
                return False
            
            with open(manifest_file, 'r') as f:
                manifest = json.load(f)
            
            databases = manifest.get('databases_backed_up', [])
            
            for database in databases:
                restore_db = target_database if target_database else database
                self.logger.info(f"Restoring database: {database} -> {restore_db}")
                
                # Create database if it doesn't exist
                try:
                    create_db_query = f"CREATE DATABASE IF NOT EXISTS {restore_db}"
                    self.client.execute(create_db_query)
                except Exception as e:
                    self.logger.error(f"Failed to create database {restore_db}: {e}")
                    continue
                
                # Restore schema
                db_backup_dir = os.path.join(backup_path, database)
                if os.path.exists(db_backup_dir):
                    self._restore_database_schema(db_backup_dir, restore_db)
                    self._restore_database_data(db_backup_dir, restore_db)
            
            self.logger.info("Restore completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Restore failed: {e}")
            return False
    
    def _restore_database_schema(self, backup_dir: str, database: str):
        """Restore database schema"""
        try:
            schema_files = [f for f in os.listdir(backup_dir) if f.endswith('_schema.sql')]
            
            for schema_file in schema_files:
                schema_path = os.path.join(backup_dir, schema_file)
                
                with open(schema_path, 'r') as f:
                    create_statement = f.read()
                
                # Modify database name if needed
                if database != self.database:
                    create_statement = create_statement.replace(f'{self.database}.', f'{database}.')
                
                try:
                    self.client.execute(create_statement)
                    self.logger.debug(f"Schema restored from: {schema_file}")
                except Exception as e:
                    self.logger.error(f"Failed to restore schema from {schema_file}: {e}")
            
        except Exception as e:
            self.logger.error(f"Schema restore failed: {e}")
    
    def _restore_database_data(self, backup_dir: str, database: str):
        """Restore database data"""
        try:
            data_files = [f for f in os.listdir(backup_dir) if f.endswith('_data.csv')]
            
            for data_file in data_files:
                table_name = data_file.replace('_data.csv', '')
                data_path = os.path.join(backup_dir, data_file)
                
                try:
                    # Insert data using clickhouse-client
                    insert_cmd = [
                        'clickhouse-client',
                        f'--host={self.host}',
                        f'--port={self.port}',
                        f'--user={self.username}',
                        '--format=CSV',
                        f'--query=INSERT INTO {database}.{table_name} FORMAT CSV'
                    ]
                    
                    if self.password:
                        insert_cmd.append(f'--password={self.password}')
                    
                    with open(data_path, 'r') as f:
                        result = subprocess.run(insert_cmd, stdin=f, stderr=subprocess.PIPE, text=True)
                    
                    if result.returncode == 0:
                        self.logger.debug(f"Data restored for table: {table_name}")
                    else:
                        self.logger.error(f"Data restore failed for {table_name}: {result.stderr}")
                
                except Exception as e:
                    self.logger.error(f"Failed to restore data for table {table_name}: {e}")
            
        except Exception as e:
            self.logger.error(f"Data restore failed: {e}")
    
    def optimize_tables(self, database: str, tables: Optional[List[str]] = None) -> Dict:
        """Optimize tables by running OPTIMIZE TABLE"""
        try:
            self.logger.info(f"Optimizing tables in database: {database}")
            
            if not tables:
                table_info = self.get_tables_info(database)
                tables = [table['name'] for table in table_info]
            
            results = {'optimized': [], 'failed': []}
            
            for table in tables:
                try:
                    self.logger.info(f"Optimizing table: {database}.{table}")
                    optimize_query = f"OPTIMIZE TABLE {database}.{table} FINAL"
                    self.client.execute(optimize_query)
                    results['optimized'].append(table)
                except Exception as e:
                    self.logger.error(f"Failed to optimize table {table}: {e}")
                    results['failed'].append({'table': table, 'error': str(e)})
            
            self.logger.info(f"Optimization completed: {len(results['optimized'])} successful, {len(results['failed'])} failed")
            return results
            
        except Exception as e:
            self.logger.error(f"Table optimization failed: {e}")
            return {'error': str(e)}
    
    def _get_directory_size(self, path: str) -> int:
        """Get directory size in bytes"""
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
        except Exception as e:
            self.logger.error(f"Failed to calculate directory size: {e}")
        return total_size
    
    def _format_bytes(self, bytes_size: int) -> str:
        """Format byte size to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} PB"
    
    def close(self):
        """Close ClickHouse connection"""
        if hasattr(self, 'client'):
            self.client.disconnect()

def main():
    parser = argparse.ArgumentParser(description='ClickHouse Backup Management Tool')
    parser.add_argument('--host', default='localhost', help='ClickHouse host')
    parser.add_argument('--port', type=int, default=9000, help='ClickHouse native port')
    parser.add_argument('--database', default='default', help='Default database')
    parser.add_argument('--username', default='default', help='Username')
    parser.add_argument('--password', default='', help='Password')
    parser.add_argument('--secure', action='store_true', help='Use secure connection')
    parser.add_argument('--backup-dir', default='/var/backups/clickhouse', help='Backup directory')
    parser.add_argument('--action', choices=['backup', 'restore', 'info', 'optimize'], 
                       default='backup', help='Action to perform')
    parser.add_argument('--databases', nargs='+', help='Specific databases to backup')
    parser.add_argument('--backup-name', help='Custom backup name')
    parser.add_argument('--restore-path', help='Backup path for restore')
    parser.add_argument('--target-database', help='Target database for restore')
    parser.add_argument('--no-data', action='store_true', help='Schema only backup')
    
    args = parser.parse_args()
    
    # Initialize backup manager
    backup_manager = ClickHouseBackup(
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.username,
        password=args.password,
        secure=args.secure
    )
    
    try:
        if args.action == 'backup':
            result = backup_manager.create_backup(
                backup_dir=args.backup_dir,
                databases=args.databases,
                backup_name=args.backup_name,
                include_data=not args.no_data
            )
            
            print(f"Backup Status: {result.get('status', 'Unknown')}")
            if result.get('errors'):
                print(f"Errors: {len(result['errors'])}")
                for error in result['errors']:
                    print(f"  - {error}")
            
            if result.get('total_size'):
                print(f"Total Size: {backup_manager._format_bytes(result['total_size'])}")
        
        elif args.action == 'restore':
            if not args.restore_path:
                print("Error: --restore-path is required for restore action")
                sys.exit(1)
            
            success = backup_manager.restore_backup(args.restore_path, args.target_database)
            print(f"Restore Status: {'Success' if success else 'Failed'}")
        
        elif args.action == 'info':
            server_info = backup_manager.get_server_info()
            databases_info = backup_manager.get_databases_info()
            
            print("\nClickHouse Server Information:")
            print("=" * 50)
            print(f"Version: {server_info.get('version', 'Unknown')}")
            print(f"Uptime: {server_info.get('uptime_days', 0)} days")
            
            print(f"\nDatabases ({len(databases_info)}):")
            for db in databases_info:
                total_rows = sum(table['rows'] for table in db['tables'])
                total_bytes = sum(table['bytes'] for table in db['tables'])
                print(f"  {db['name']}: {len(db['tables'])} tables, {total_rows:,} rows, {backup_manager._format_bytes(total_bytes)}")
        
        elif args.action == 'optimize':
            if not args.databases:
                args.databases = [args.database]
            
            for database in args.databases:
                result = backup_manager.optimize_tables(database)
                print(f"Optimization for {database}: {len(result.get('optimized', []))} successful, {len(result.get('failed', []))} failed")
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        backup_manager.logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        backup_manager.close()

if __name__ == "__main__":
    main()