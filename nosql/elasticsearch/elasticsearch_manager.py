#!/usr/bin/env python3
"""
Elasticsearch Management Script (Python)
Author: DBA Portfolio
Purpose: Comprehensive Elasticsearch cluster management and backup
"""

import os
import sys
import json
import time
import argparse
import logging
import datetime
import requests
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth

class ElasticsearchManager:
    def __init__(self, hosts: List[str], username: Optional[str] = None, 
                 password: Optional[str] = None, use_ssl: bool = False,
                 verify_ssl: bool = True):
        self.hosts = [host.rstrip('/') for host in hosts]
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        
        # Setup authentication
        if username and password:
            self.session.auth = HTTPBasicAuth(username, password)
        
        # SSL configuration
        if not verify_ssl:
            self.session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self.setup_logging()
        self.base_url = self._get_active_host()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'elasticsearch_manager_{datetime.datetime.now().strftime("%Y%m%d")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _get_active_host(self) -> str:
        """Find an active Elasticsearch host"""
        protocol = 'https' if self.use_ssl else 'http'
        
        for host in self.hosts:
            if not host.startswith(('http://', 'https://')):
                host = f"{protocol}://{host}"
            
            try:
                response = self.session.get(f"{host}/", timeout=5)
                if response.status_code == 200:
                    self.logger.info(f"Connected to Elasticsearch at {host}")
                    return host
            except requests.exceptions.RequestException:
                continue
        
        raise Exception(f"Could not connect to any Elasticsearch hosts: {self.hosts}")
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request to Elasticsearch"""
        url = urljoin(self.base_url, endpoint.lstrip('/'))
        
        try:
            response = self.session.request(method, url, timeout=30, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {method} {url} - {e}")
            raise
    
    def get_cluster_info(self) -> Dict:
        """Get cluster information"""
        try:
            # Get basic cluster info
            cluster_info = self._make_request('GET', '/').json()
            
            # Get cluster health
            health = self._make_request('GET', '/_cluster/health').json()
            
            # Get cluster stats
            stats = self._make_request('GET', '/_cluster/stats').json()
            
            # Get nodes info
            nodes = self._make_request('GET', '/_nodes').json()
            
            return {
                'cluster_info': cluster_info,
                'health': health,
                'stats': stats,
                'nodes': nodes,
                'summary': {
                    'cluster_name': cluster_info.get('cluster_name', 'Unknown'),
                    'version': cluster_info.get('version', {}).get('number', 'Unknown'),
                    'status': health.get('status', 'Unknown'),
                    'number_of_nodes': health.get('number_of_nodes', 0),
                    'number_of_data_nodes': health.get('number_of_data_nodes', 0),
                    'active_primary_shards': health.get('active_primary_shards', 0),
                    'active_shards': health.get('active_shards', 0),
                    'relocating_shards': health.get('relocating_shards', 0),
                    'initializing_shards': health.get('initializing_shards', 0),
                    'unassigned_shards': health.get('unassigned_shards', 0),
                    'number_of_indices': stats.get('indices', {}).get('count', 0),
                    'total_docs': stats.get('indices', {}).get('docs', {}).get('count', 0),
                    'store_size': stats.get('indices', {}).get('store', {}).get('size_in_bytes', 0)
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get cluster info: {e}")
            return {}
    
    def get_indices_info(self, index_pattern: str = "*") -> List[Dict]:
        """Get information about indices"""
        try:
            # Get indices stats
            stats_response = self._make_request('GET', f'/{index_pattern}/_stats')
            stats_data = stats_response.json()
            
            # Get indices settings
            settings_response = self._make_request('GET', f'/{index_pattern}/_settings')
            settings_data = settings_response.json()
            
            # Get indices mappings
            mappings_response = self._make_request('GET', f'/{index_pattern}/_mapping')
            mappings_data = mappings_response.json()
            
            indices = []
            
            for index_name, index_stats in stats_data.get('indices', {}).items():
                # Skip system indices unless specifically requested
                if index_name.startswith('.') and index_pattern == "*":
                    continue
                
                index_settings = settings_data.get(index_name, {}).get('settings', {})
                index_mappings = mappings_data.get(index_name, {}).get('mappings', {})
                
                index_info = {
                    'name': index_name,
                    'health': self._get_index_health(index_name),
                    'status': index_settings.get('index', {}).get('status', 'open'),
                    'number_of_shards': int(index_settings.get('index', {}).get('number_of_shards', 1)),
                    'number_of_replicas': int(index_settings.get('index', {}).get('number_of_replicas', 1)),
                    'docs_count': index_stats.get('total', {}).get('docs', {}).get('count', 0),
                    'docs_deleted': index_stats.get('total', {}).get('docs', {}).get('deleted', 0),
                    'store_size_bytes': index_stats.get('total', {}).get('store', {}).get('size_in_bytes', 0),
                    'store_size_human': self._format_bytes(index_stats.get('total', {}).get('store', {}).get('size_in_bytes', 0)),
                    'creation_date': index_settings.get('index', {}).get('creation_date'),
                    'mappings': index_mappings,
                    'settings': index_settings
                }
                
                indices.append(index_info)
            
            # Sort by size
            indices.sort(key=lambda x: x['store_size_bytes'], reverse=True)
            return indices
            
        except Exception as e:
            self.logger.error(f"Failed to get indices info: {e}")
            return []
    
    def _get_index_health(self, index_name: str) -> str:
        """Get health status for specific index"""
        try:
            response = self._make_request('GET', f'/_cluster/health/{index_name}')
            return response.json().get('status', 'Unknown')
        except:
            return 'Unknown'
    
    def create_snapshot_repository(self, repo_name: str, repo_type: str = "fs", 
                                  location: str = None, **settings) -> bool:
        """Create snapshot repository"""
        try:
            self.logger.info(f"Creating snapshot repository: {repo_name}")
            
            if not location and repo_type == "fs":
                location = f"/var/lib/elasticsearch/snapshots/{repo_name}"
            
            repo_settings = {
                "type": repo_type,
                "settings": {
                    "location": location,
                    **settings
                }
            }
            
            response = self._make_request('PUT', f'/_snapshot/{repo_name}', 
                                        json=repo_settings)
            
            if response.status_code in [200, 201]:
                self.logger.info(f"Repository created successfully: {repo_name}")
                return True
            else:
                self.logger.error(f"Failed to create repository: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to create repository: {e}")
            return False
    
    def create_snapshot(self, repo_name: str, snapshot_name: str, 
                       indices: Optional[str] = None, ignore_unavailable: bool = True,
                       include_global_state: bool = True) -> Dict:
        """Create snapshot"""
        try:
            self.logger.info(f"Creating snapshot: {snapshot_name} in repository: {repo_name}")
            
            snapshot_body = {
                "ignore_unavailable": ignore_unavailable,
                "include_global_state": include_global_state
            }
            
            if indices:
                snapshot_body["indices"] = indices
            
            response = self._make_request('PUT', f'/_snapshot/{repo_name}/{snapshot_name}', 
                                        json=snapshot_body, params={"wait_for_completion": "false"})
            
            if response.status_code in [200, 201, 202]:
                self.logger.info(f"Snapshot creation initiated: {snapshot_name}")
                
                # Wait for completion or return status
                return self._wait_for_snapshot_completion(repo_name, snapshot_name)
            else:
                return {
                    'status': 'failed',
                    'error': response.text
                }
                
        except Exception as e:
            self.logger.error(f"Failed to create snapshot: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def _wait_for_snapshot_completion(self, repo_name: str, snapshot_name: str, 
                                    timeout: int = 3600) -> Dict:
        """Wait for snapshot to complete"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                response = self._make_request('GET', f'/_snapshot/{repo_name}/{snapshot_name}')
                snapshot_data = response.json()
                
                snapshots = snapshot_data.get('snapshots', [])
                if snapshots:
                    snapshot = snapshots[0]
                    state = snapshot.get('state')
                    
                    if state == 'SUCCESS':
                        self.logger.info(f"Snapshot completed successfully: {snapshot_name}")
                        return {
                            'status': 'success',
                            'snapshot': snapshot
                        }
                    elif state == 'FAILED':
                        self.logger.error(f"Snapshot failed: {snapshot.get('reason', 'Unknown reason')}")
                        return {
                            'status': 'failed',
                            'error': snapshot.get('reason', 'Unknown reason')
                        }
                    elif state in ['IN_PROGRESS', 'STARTED']:
                        # Log progress if available
                        shards = snapshot.get('shards', {})
                        if shards:
                            total = shards.get('total', 0)
                            successful = shards.get('successful', 0)
                            if total > 0:
                                progress = (successful / total) * 100
                                self.logger.info(f"Snapshot progress: {progress:.1f}% ({successful}/{total} shards)")
                        
                        time.sleep(30)  # Wait 30 seconds before checking again
                    else:
                        self.logger.warning(f"Unknown snapshot state: {state}")
                        time.sleep(30)
                
            except Exception as e:
                self.logger.error(f"Error checking snapshot status: {e}")
                time.sleep(30)
        
        self.logger.error(f"Snapshot creation timed out: {snapshot_name}")
        return {
            'status': 'timeout',
            'error': 'Snapshot creation timed out'
        }
    
    def list_snapshots(self, repo_name: str) -> List[Dict]:
        """List snapshots in repository"""
        try:
            response = self._make_request('GET', f'/_snapshot/{repo_name}/_all')
            snapshot_data = response.json()
            
            snapshots = []
            for snapshot in snapshot_data.get('snapshots', []):
                snapshot_info = {
                    'name': snapshot.get('snapshot'),
                    'state': snapshot.get('state'),
                    'start_time': snapshot.get('start_time'),
                    'end_time': snapshot.get('end_time'),
                    'duration_in_millis': snapshot.get('duration_in_millis'),
                    'indices': snapshot.get('indices', []),
                    'shards': snapshot.get('shards', {}),
                    'size_in_bytes': snapshot.get('size_in_bytes', 0),
                    'size_human': self._format_bytes(snapshot.get('size_in_bytes', 0))
                }
                snapshots.append(snapshot_info)
            
            # Sort by creation time
            snapshots.sort(key=lambda x: x['start_time'], reverse=True)
            return snapshots
            
        except Exception as e:
            self.logger.error(f"Failed to list snapshots: {e}")
            return []
    
    def delete_snapshot(self, repo_name: str, snapshot_name: str) -> bool:
        """Delete snapshot"""
        try:
            self.logger.info(f"Deleting snapshot: {snapshot_name}")
            
            response = self._make_request('DELETE', f'/_snapshot/{repo_name}/{snapshot_name}')
            
            if response.status_code == 200:
                self.logger.info(f"Snapshot deleted successfully: {snapshot_name}")
                return True
            else:
                self.logger.error(f"Failed to delete snapshot: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to delete snapshot: {e}")
            return False
    
    def reindex_data(self, source_index: str, dest_index: str, 
                    query: Optional[Dict] = None) -> Dict:
        """Reindex data from source to destination"""
        try:
            self.logger.info(f"Reindexing from {source_index} to {dest_index}")
            
            reindex_body = {
                "source": {
                    "index": source_index
                },
                "dest": {
                    "index": dest_index
                }
            }
            
            if query:
                reindex_body["source"]["query"] = query
            
            response = self._make_request('POST', '/_reindex', json=reindex_body,
                                        params={"wait_for_completion": "false"})
            
            if response.status_code in [200, 201]:
                task_data = response.json()
                task_id = task_data.get('task')
                
                if task_id:
                    self.logger.info(f"Reindex task started: {task_id}")
                    return {
                        'status': 'started',
                        'task_id': task_id
                    }
                else:
                    return {
                        'status': 'completed',
                        'result': task_data
                    }
            else:
                return {
                    'status': 'failed',
                    'error': response.text
                }
                
        except Exception as e:
            self.logger.error(f"Failed to start reindex: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def get_task_status(self, task_id: str) -> Dict:
        """Get task status"""
        try:
            response = self._make_request('GET', f'/_tasks/{task_id}')
            return response.json()
        except Exception as e:
            self.logger.error(f"Failed to get task status: {e}")
            return {}
    
    def optimize_indices(self, index_pattern: str = "*", max_num_segments: int = 1) -> Dict:
        """Optimize indices by forcing merge"""
        try:
            self.logger.info(f"Optimizing indices: {index_pattern}")
            
            response = self._make_request('POST', f'/{index_pattern}/_forcemerge',
                                        params={"max_num_segments": max_num_segments})
            
            if response.status_code == 200:
                self.logger.info("Index optimization completed")
                return {
                    'status': 'success',
                    'result': response.json()
                }
            else:
                return {
                    'status': 'failed',
                    'error': response.text
                }
                
        except Exception as e:
            self.logger.error(f"Failed to optimize indices: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def cleanup_old_snapshots(self, repo_name: str, retention_days: int) -> int:
        """Clean up old snapshots"""
        try:
            snapshots = self.list_snapshots(repo_name)
            cutoff_time = datetime.datetime.now() - datetime.timedelta(days=retention_days)
            deleted_count = 0
            
            for snapshot in snapshots:
                if snapshot['start_time']:
                    # Parse snapshot start time
                    snapshot_time = datetime.datetime.strptime(
                        snapshot['start_time'], '%Y-%m-%dT%H:%M:%S.%fZ'
                    )
                    
                    if snapshot_time < cutoff_time:
                        if self.delete_snapshot(repo_name, snapshot['name']):
                            deleted_count += 1
            
            self.logger.info(f"Cleaned up {deleted_count} old snapshots")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup snapshots: {e}")
            return 0
    
    def _format_bytes(self, bytes_size: int) -> str:
        """Format byte size to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} PB"
    
    def generate_health_report(self) -> Dict:
        """Generate comprehensive health report"""
        self.logger.info("Generating Elasticsearch health report")
        
        report = {
            'timestamp': datetime.datetime.now().isoformat(),
            'cluster_info': self.get_cluster_info(),
            'indices': self.get_indices_info(),
            'health_checks': [],
            'recommendations': []
        }
        
        # Perform health checks
        cluster_summary = report['cluster_info'].get('summary', {})
        
        # Check cluster status
        status = cluster_summary.get('status', 'red')
        if status == 'red':
            report['health_checks'].append({
                'type': 'CRITICAL',
                'category': 'Cluster Health',
                'message': 'Cluster status is RED - immediate attention required',
                'recommendation': 'Check for unassigned shards and node failures'
            })
        elif status == 'yellow':
            report['health_checks'].append({
                'type': 'WARNING',
                'category': 'Cluster Health', 
                'message': 'Cluster status is YELLOW - some replicas are unassigned',
                'recommendation': 'Check shard allocation and node capacity'
            })
        
        # Check for unassigned shards
        unassigned = cluster_summary.get('unassigned_shards', 0)
        if unassigned > 0:
            report['health_checks'].append({
                'type': 'WARNING',
                'category': 'Shard Allocation',
                'message': f'{unassigned} unassigned shards found',
                'recommendation': 'Check node capacity and allocation settings'
            })
        
        # Check large indices
        for index in report['indices']:
            if index['store_size_bytes'] > 50 * 1024**3:  # 50GB
                report['recommendations'].append({
                    'type': 'INFO',
                    'category': 'Index Management',
                    'message': f"Large index detected: {index['name']} ({index['store_size_human']})",
                    'recommendation': 'Consider index lifecycle management or archival'
                })
        
        return report

def main():
    parser = argparse.ArgumentParser(description='Elasticsearch Management Tool')
    parser.add_argument('--hosts', nargs='+', default=['localhost:9200'], 
                       help='Elasticsearch host addresses')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--ssl', action='store_true', help='Use SSL/HTTPS')
    parser.add_argument('--verify-ssl', action='store_false', help='Skip SSL verification')
    parser.add_argument('--action', choices=['info', 'indices', 'snapshot', 'list-snapshots', 
                                           'cleanup', 'optimize', 'health'], 
                       default='info', help='Action to perform')
    parser.add_argument('--repo-name', default='backup_repo', help='Snapshot repository name')
    parser.add_argument('--snapshot-name', help='Snapshot name')
    parser.add_argument('--repo-location', help='Repository location for filesystem repos')
    parser.add_argument('--indices', default='*', help='Index pattern')
    parser.add_argument('--retention-days', type=int, default=7, help='Retention days for cleanup')
    
    args = parser.parse_args()
    
    # Initialize manager
    manager = ElasticsearchManager(
        hosts=args.hosts,
        username=args.username,
        password=args.password,
        use_ssl=args.ssl,
        verify_ssl=args.verify_ssl
    )
    
    try:
        if args.action == 'info':
            cluster_info = manager.get_cluster_info()
            summary = cluster_info.get('summary', {})
            
            print("\nElasticsearch Cluster Information:")
            print("=" * 50)
            print(f"Cluster Name: {summary.get('cluster_name', 'Unknown')}")
            print(f"Version: {summary.get('version', 'Unknown')}")
            print(f"Status: {summary.get('status', 'Unknown')}")
            print(f"Nodes: {summary.get('number_of_nodes', 0)} ({summary.get('number_of_data_nodes', 0)} data nodes)")
            print(f"Indices: {summary.get('number_of_indices', 0)}")
            print(f"Documents: {summary.get('total_docs', 0):,}")
            print(f"Store Size: {manager._format_bytes(summary.get('store_size', 0))}")
            print(f"Active Shards: {summary.get('active_shards', 0)}")
            print(f"Unassigned Shards: {summary.get('unassigned_shards', 0)}")
        
        elif args.action == 'indices':
            indices = manager.get_indices_info(args.indices)
            
            print(f"\nIndices Information ({len(indices)} indices):")
            print("=" * 80)
            for index in indices:
                print(f"Name: {index['name']}")
                print(f"  Status: {index['health']} | Docs: {index['docs_count']:,} | Size: {index['store_size_human']}")
                print(f"  Shards: {index['number_of_shards']} | Replicas: {index['number_of_replicas']}")
                print()
        
        elif args.action == 'snapshot':
            if not args.snapshot_name:
                args.snapshot_name = f"snapshot_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create repository if it doesn't exist
            manager.create_snapshot_repository(args.repo_name, location=args.repo_location)
            
            # Create snapshot
            result = manager.create_snapshot(args.repo_name, args.snapshot_name, args.indices)
            print(f"Snapshot Status: {result['status']}")
            if result['status'] != 'success':
                print(f"Error: {result.get('error', 'Unknown error')}")
        
        elif args.action == 'list-snapshots':
            snapshots = manager.list_snapshots(args.repo_name)
            
            print(f"\nSnapshots in repository '{args.repo_name}' ({len(snapshots)} snapshots):")
            print("=" * 80)
            for snapshot in snapshots:
                print(f"Name: {snapshot['name']}")
                print(f"  State: {snapshot['state']} | Size: {snapshot['size_human']}")
                print(f"  Start: {snapshot['start_time']} | Duration: {snapshot.get('duration_in_millis', 0)/1000:.1f}s")
                print(f"  Indices: {len(snapshot['indices'])} indices")
                print()
        
        elif args.action == 'cleanup':
            deleted = manager.cleanup_old_snapshots(args.repo_name, args.retention_days)
            print(f"Cleaned up {deleted} old snapshots")
        
        elif args.action == 'optimize':
            result = manager.optimize_indices(args.indices)
            print(f"Optimization Status: {result['status']}")
        
        elif args.action == 'health':
            report = manager.generate_health_report()
            
            print("\nElasticsearch Health Report:")
            print("=" * 50)
            summary = report['cluster_info'].get('summary', {})
            print(f"Cluster Status: {summary.get('status', 'Unknown')}")
            
            if report['health_checks']:
                print(f"\nHealth Checks ({len(report['health_checks'])} issues):")
                for check in report['health_checks']:
                    print(f"  {check['type']}: {check['message']}")
            
            if report['recommendations']:
                print(f"\nRecommendations ({len(report['recommendations'])}):")
                for rec in report['recommendations']:
                    print(f"  - {rec['message']}")
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        manager.logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()