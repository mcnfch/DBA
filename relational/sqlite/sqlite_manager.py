#!/usr/bin/env python3
"""
SQLite Database Management Script (Python)
Author: DBA Portfolio
Purpose: Comprehensive SQLite database administration and maintenance
"""

import sqlite3
import os
import sys
import json
import shutil
import argparse
import logging
import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class SQLiteManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'sqlite_manager_{datetime.datetime.now().strftime("%Y%m%d")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> sqlite3.Connection:
        """Create database connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def backup_database(self, backup_path: str, compress: bool = True) -> bool:
        """Create database backup"""
        try:
            self.logger.info(f"Starting backup of {self.db_path}")
            
            # Ensure backup directory exists
            Path(backup_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Create backup using sqlite3 backup API
            source = sqlite3.connect(self.db_path)
            backup = sqlite3.connect(backup_path)
            
            source.backup(backup)
            source.close()
            backup.close()
            
            # Get file sizes
            original_size = os.path.getsize(self.db_path)
            backup_size = os.path.getsize(backup_path)
            
            self.logger.info(f"Backup completed: {backup_path}")
            self.logger.info(f"Original size: {self._format_bytes(original_size)}")
            self.logger.info(f"Backup size: {self._format_bytes(backup_size)}")
            
            # Compress if requested
            if compress and backup_path.endswith('.db'):
                compressed_path = backup_path + '.gz'
                import gzip
                with open(backup_path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                
                os.remove(backup_path)
                compressed_size = os.path.getsize(compressed_path)
                self.logger.info(f"Compressed backup: {compressed_path}")
                self.logger.info(f"Compressed size: {self._format_bytes(compressed_size)}")
                self.logger.info(f"Compression ratio: {compressed_size/backup_size:.2%}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
            return False
    
    def analyze_database(self) -> Dict:
        """Analyze database structure and statistics"""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            analysis = {
                'database_info': self._get_database_info(),
                'tables': [],
                'indexes': [],
                'views': [],
                'triggers': [],
                'statistics': {}
            }
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()
            
            total_rows = 0
            for table in tables:
                table_name = table['name']
                table_info = self._analyze_table(cursor, table_name)
                analysis['tables'].append(table_info)
                total_rows += table_info.get('row_count', 0)
            
            # Get indexes
            cursor.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")
            for index in cursor.fetchall():
                analysis['indexes'].append({
                    'name': index['name'],
                    'table': index['tbl_name'],
                    'sql': index['sql']
                })
            
            # Get views
            cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='view'")
            for view in cursor.fetchall():
                analysis['views'].append({
                    'name': view['name'],
                    'sql': view['sql']
                })
            
            # Get triggers
            cursor.execute("SELECT name, tbl_name, sql FROM sqlite_master WHERE type='trigger'")
            for trigger in cursor.fetchall():
                analysis['triggers'].append({
                    'name': trigger['name'],
                    'table': trigger['tbl_name'],
                    'sql': trigger['sql']
                })
            
            # Database statistics
            analysis['statistics'] = {
                'total_tables': len(analysis['tables']),
                'total_indexes': len(analysis['indexes']),
                'total_views': len(analysis['views']),
                'total_triggers': len(analysis['triggers']),
                'total_rows': total_rows,
                'database_size': self._format_bytes(os.path.getsize(self.db_path)) if os.path.exists(self.db_path) else "0 B"
            }
            
            conn.close()
            return analysis
            
        except Exception as e:
            self.logger.error(f"Database analysis failed: {e}")
            return {}
    
    def _analyze_table(self, cursor: sqlite3.Cursor, table_name: str) -> Dict:
        """Analyze individual table"""
        try:
            table_info = {
                'name': table_name,
                'columns': [],
                'row_count': 0,
                'indexes': [],
                'constraints': []
            }
            
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            for col in columns:
                table_info['columns'].append({
                    'name': col['name'],
                    'type': col['type'],
                    'not_null': bool(col['notnull']),
                    'default': col['dflt_value'],
                    'primary_key': bool(col['pk'])
                })
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            table_info['row_count'] = cursor.fetchone()['count']
            
            # Get table indexes
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            for idx in indexes:
                cursor.execute(f"PRAGMA index_info({idx['name']})")
                index_columns = cursor.fetchall()
                table_info['indexes'].append({
                    'name': idx['name'],
                    'unique': bool(idx['unique']),
                    'columns': [col['name'] for col in index_columns]
                })
            
            # Get foreign keys
            cursor.execute(f"PRAGMA foreign_key_list({table_name})")
            foreign_keys = cursor.fetchall()
            for fk in foreign_keys:
                table_info['constraints'].append({
                    'type': 'foreign_key',
                    'column': fk['from'],
                    'references': f"{fk['table']}.{fk['to']}"
                })
            
            return table_info
            
        except Exception as e:
            self.logger.error(f"Table analysis failed for {table_name}: {e}")
            return {'name': table_name, 'error': str(e)}
    
    def _get_database_info(self) -> Dict:
        """Get general database information"""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            info = {}
            
            # Get SQLite version
            cursor.execute("SELECT sqlite_version() as version")
            info['sqlite_version'] = cursor.fetchone()['version']
            
            # Get pragma information
            pragmas = [
                'journal_mode', 'synchronous', 'cache_size', 'page_size',
                'auto_vacuum', 'foreign_keys', 'encoding'
            ]
            
            for pragma in pragmas:
                try:
                    cursor.execute(f"PRAGMA {pragma}")
                    result = cursor.fetchone()
                    info[pragma] = result[0] if result else None
                except:
                    info[pragma] = None
            
            # Get file info
            if os.path.exists(self.db_path):
                stat = os.stat(self.db_path)
                info['file_size'] = stat.st_size
                info['file_size_formatted'] = self._format_bytes(stat.st_size)
                info['last_modified'] = datetime.datetime.fromtimestamp(stat.st_mtime).isoformat()
            
            conn.close()
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get database info: {e}")
            return {}
    
    def vacuum_database(self) -> bool:
        """Vacuum database to reclaim space"""
        try:
            self.logger.info("Starting database vacuum operation")
            
            # Get size before vacuum
            size_before = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            
            conn = self.connect()
            cursor = conn.cursor()
            
            # Execute VACUUM
            cursor.execute("VACUUM")
            conn.commit()
            conn.close()
            
            # Get size after vacuum
            size_after = os.path.getsize(self.db_path)
            space_saved = size_before - size_after
            
            self.logger.info("Vacuum operation completed")
            self.logger.info(f"Size before: {self._format_bytes(size_before)}")
            self.logger.info(f"Size after: {self._format_bytes(size_after)}")
            self.logger.info(f"Space saved: {self._format_bytes(space_saved)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Vacuum operation failed: {e}")
            return False
    
    def integrity_check(self) -> Dict:
        """Check database integrity"""
        try:
            self.logger.info("Starting integrity check")
            
            conn = self.connect()
            cursor = conn.cursor()
            
            results = {
                'integrity_check': [],
                'foreign_key_check': [],
                'quick_check': []
            }
            
            # Integrity check
            cursor.execute("PRAGMA integrity_check")
            integrity_results = cursor.fetchall()
            results['integrity_check'] = [row[0] for row in integrity_results]
            
            # Foreign key check
            cursor.execute("PRAGMA foreign_key_check")
            fk_results = cursor.fetchall()
            results['foreign_key_check'] = [dict(row) for row in fk_results]
            
            # Quick check
            cursor.execute("PRAGMA quick_check")
            quick_results = cursor.fetchall()
            results['quick_check'] = [row[0] for row in quick_results]
            
            conn.close()
            
            # Determine overall status
            is_healthy = (
                len(results['integrity_check']) == 1 and results['integrity_check'][0] == 'ok' and
                len(results['foreign_key_check']) == 0 and
                len(results['quick_check']) == 1 and results['quick_check'][0] == 'ok'
            )
            
            results['status'] = 'HEALTHY' if is_healthy else 'ISSUES_FOUND'
            
            self.logger.info(f"Integrity check completed: {results['status']}")
            return results
            
        except Exception as e:
            self.logger.error(f"Integrity check failed: {e}")
            return {'status': 'ERROR', 'error': str(e)}
    
    def optimize_database(self) -> Dict:
        """Optimize database performance"""
        try:
            self.logger.info("Starting database optimization")
            
            conn = self.connect()
            cursor = conn.cursor()
            
            optimizations = {
                'analyze_completed': False,
                'pragma_optimizations': [],
                'suggestions': []
            }
            
            # Run ANALYZE to update statistics
            cursor.execute("ANALYZE")
            optimizations['analyze_completed'] = True
            
            # Check and suggest pragma optimizations
            cursor.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            if journal_mode != 'WAL':
                optimizations['suggestions'].append({
                    'type': 'pragma',
                    'setting': 'journal_mode=WAL',
                    'reason': 'WAL mode provides better concurrency and performance'
                })
            
            cursor.execute("PRAGMA synchronous")
            synchronous = cursor.fetchone()[0]
            if synchronous == 2:  # FULL
                optimizations['suggestions'].append({
                    'type': 'pragma',
                    'setting': 'synchronous=NORMAL',
                    'reason': 'NORMAL synchronous mode is faster while still safe'
                })
            
            cursor.execute("PRAGMA cache_size")
            cache_size = cursor.fetchone()[0]
            if abs(cache_size) < 2000:  # Default is often -2000 (2MB)
                optimizations['suggestions'].append({
                    'type': 'pragma',
                    'setting': 'cache_size=-10000',
                    'reason': 'Larger cache size (10MB) improves performance'
                })
            
            # Check for missing indexes on large tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()
            
            for table in tables:
                table_name = table['name']
                cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                row_count = cursor.fetchone()['count']
                
                if row_count > 1000:  # Only check larger tables
                    # Check if table has indexes
                    cursor.execute(f"PRAGMA index_list({table_name})")
                    indexes = cursor.fetchall()
                    
                    if not indexes:
                        optimizations['suggestions'].append({
                            'type': 'index',
                            'table': table_name,
                            'reason': f'Table {table_name} has {row_count} rows but no indexes'
                        })
            
            conn.close()
            
            self.logger.info(f"Optimization analysis completed. Found {len(optimizations['suggestions'])} suggestions")
            return optimizations
            
        except Exception as e:
            self.logger.error(f"Database optimization failed: {e}")
            return {'error': str(e)}
    
    def export_to_sql(self, output_path: str) -> bool:
        """Export database schema and data to SQL file"""
        try:
            self.logger.info(f"Exporting database to SQL: {output_path}")
            
            conn = self.connect()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write(f"-- SQLite Database Export\n")
                f.write(f"-- Generated: {datetime.datetime.now().isoformat()}\n")
                f.write(f"-- Source: {self.db_path}\n\n")
                
                # Export schema and data
                for line in conn.iterdump():
                    f.write(f"{line}\n")
            
            conn.close()
            
            export_size = os.path.getsize(output_path)
            self.logger.info(f"Export completed: {self._format_bytes(export_size)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            return False
    
    def _format_bytes(self, bytes_size: int) -> str:
        """Format byte size to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} PB"
    
    def generate_report(self, output_file: Optional[str] = None) -> Dict:
        """Generate comprehensive database report"""
        self.logger.info("Generating comprehensive database report")
        
        report = {
            'timestamp': datetime.datetime.now().isoformat(),
            'database_path': self.db_path,
            'analysis': self.analyze_database(),
            'integrity_check': self.integrity_check(),
            'optimization_suggestions': self.optimize_database()
        }
        
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"Report saved to: {output_file}")
        
        return report

def main():
    parser = argparse.ArgumentParser(description='SQLite Database Management Tool')
    parser.add_argument('database', help='Path to SQLite database file')
    parser.add_argument('--action', choices=['analyze', 'backup', 'vacuum', 'integrity', 'optimize', 'export', 'report'], 
                       default='analyze', help='Action to perform')
    parser.add_argument('--backup-path', help='Backup file path')
    parser.add_argument('--export-path', help='SQL export file path') 
    parser.add_argument('--report-path', help='JSON report file path')
    parser.add_argument('--compress', action='store_true', help='Compress backup files')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.database):
        print(f"Error: Database file not found: {args.database}")
        sys.exit(1)
    
    manager = SQLiteManager(args.database)
    
    try:
        if args.action == 'analyze':
            analysis = manager.analyze_database()
            print("\nDatabase Analysis:")
            print("="*50)
            print(f"Database: {args.database}")
            print(f"Size: {analysis.get('statistics', {}).get('database_size', 'Unknown')}")
            print(f"Tables: {analysis.get('statistics', {}).get('total_tables', 0)}")
            print(f"Indexes: {analysis.get('statistics', {}).get('total_indexes', 0)}")
            print(f"Views: {analysis.get('statistics', {}).get('total_views', 0)}")
            print(f"Total Rows: {analysis.get('statistics', {}).get('total_rows', 0)}")
            
            # Show largest tables
            tables = analysis.get('tables', [])
            if tables:
                print(f"\nLargest Tables:")
                sorted_tables = sorted(tables, key=lambda x: x.get('row_count', 0), reverse=True)[:5]
                for table in sorted_tables:
                    print(f"  {table['name']}: {table.get('row_count', 0):,} rows")
        
        elif args.action == 'backup':
            if not args.backup_path:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                args.backup_path = f"{args.database}.backup_{timestamp}.db"
            
            success = manager.backup_database(args.backup_path, args.compress)
            if success:
                print(f"Backup completed: {args.backup_path}")
            else:
                print("Backup failed")
                sys.exit(1)
        
        elif args.action == 'vacuum':
            success = manager.vacuum_database()
            if not success:
                sys.exit(1)
        
        elif args.action == 'integrity':
            results = manager.integrity_check()
            print(f"Integrity Check: {results['status']}")
            if results['status'] != 'HEALTHY':
                print("Issues found:")
                for check, issues in results.items():
                    if isinstance(issues, list) and issues and check != 'status':
                        print(f"  {check}: {issues}")
        
        elif args.action == 'optimize':
            optimizations = manager.optimize_database()
            print(f"Optimization Analysis:")
            print(f"Suggestions: {len(optimizations.get('suggestions', []))}")
            for suggestion in optimizations.get('suggestions', []):
                print(f"  - {suggestion.get('reason', 'No reason provided')}")
        
        elif args.action == 'export':
            if not args.export_path:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                args.export_path = f"{args.database}.export_{timestamp}.sql"
            
            success = manager.export_to_sql(args.export_path)
            if success:
                print(f"Export completed: {args.export_path}")
            else:
                print("Export failed")
                sys.exit(1)
        
        elif args.action == 'report':
            report = manager.generate_report(args.report_path)
            if args.report_path:
                print(f"Report generated: {args.report_path}")
            else:
                print("\nDatabase Report Summary:")
                print("="*50)
                print(json.dumps(report, indent=2, default=str))
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        manager.logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()