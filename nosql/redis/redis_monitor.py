#!/usr/bin/env python3
"""
Redis Monitoring and Management Script (Python)
Author: DBA Portfolio
Purpose: Comprehensive Redis cluster and standalone monitoring
"""

import redis
import sys
import json
import time
import argparse
import logging
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta

class RedisMonitor:
    def __init__(self, host: str = 'localhost', port: int = 6379, 
                 password: Optional[str] = None, db: int = 0, 
                 cluster_mode: bool = False):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.cluster_mode = cluster_mode
        self.setup_logging()
        self.connect_to_redis()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'redis_monitor_{datetime.now().strftime("%Y%m%d")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect_to_redis(self):
        """Connect to Redis server or cluster"""
        try:
            if self.cluster_mode:
                from redis.cluster import RedisCluster
                startup_nodes = [{"host": self.host, "port": self.port}]
                self.redis_client = RedisCluster(
                    startup_nodes=startup_nodes,
                    decode_responses=True,
                    password=self.password,
                    skip_full_coverage_check=True
                )
                self.logger.info(f"Connected to Redis cluster at {self.host}:{self.port}")
            else:
                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    db=self.db,
                    decode_responses=True,
                    socket_connect_timeout=5
                )
                self.logger.info(f"Connected to Redis server at {self.host}:{self.port}")
            
            # Test connection
            self.redis_client.ping()
            
        except redis.ConnectionError as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to Redis: {e}")
            sys.exit(1)
    
    def get_server_info(self) -> Dict:
        """Get comprehensive server information"""
        try:
            info = self.redis_client.info()
            
            server_info = {
                'redis_version': info.get('redis_version', 'Unknown'),
                'redis_mode': info.get('redis_mode', 'standalone'),
                'os': info.get('os', 'Unknown'),
                'arch_bits': info.get('arch_bits', 'Unknown'),
                'process_id': info.get('process_id', 'Unknown'),
                'uptime_in_seconds': info.get('uptime_in_seconds', 0),
                'uptime_in_days': info.get('uptime_in_days', 0),
                'config_file': info.get('config_file', 'Unknown')
            }
            
            return server_info
            
        except Exception as e:
            self.logger.error(f"Failed to get server info: {e}")
            return {}
    
    def get_memory_info(self) -> Dict:
        """Get memory usage information"""
        try:
            info = self.redis_client.info('memory')
            
            memory_info = {
                'used_memory': info.get('used_memory', 0),
                'used_memory_human': info.get('used_memory_human', '0B'),
                'used_memory_rss': info.get('used_memory_rss', 0),
                'used_memory_rss_human': info.get('used_memory_rss_human', '0B'),
                'used_memory_peak': info.get('used_memory_peak', 0),
                'used_memory_peak_human': info.get('used_memory_peak_human', '0B'),
                'maxmemory': info.get('maxmemory', 0),
                'maxmemory_human': info.get('maxmemory_human', 'Unknown'),
                'maxmemory_policy': info.get('maxmemory_policy', 'noeviction'),
                'mem_fragmentation_ratio': info.get('mem_fragmentation_ratio', 0.0),
                'mem_allocator': info.get('mem_allocator', 'Unknown')
            }
            
            # Calculate memory usage percentage
            if memory_info['maxmemory'] > 0:
                memory_info['memory_usage_percent'] = round(
                    (memory_info['used_memory'] / memory_info['maxmemory']) * 100, 2
                )
            else:
                memory_info['memory_usage_percent'] = 0.0
            
            return memory_info
            
        except Exception as e:
            self.logger.error(f"Failed to get memory info: {e}")
            return {}
    
    def get_client_info(self) -> Dict:
        """Get client connection information"""
        try:
            info = self.redis_client.info('clients')
            
            client_info = {
                'connected_clients': info.get('connected_clients', 0),
                'client_recent_max_input_buffer': info.get('client_recent_max_input_buffer', 0),
                'client_recent_max_output_buffer': info.get('client_recent_max_output_buffer', 0),
                'blocked_clients': info.get('blocked_clients', 0),
                'tracking_clients': info.get('tracking_clients', 0)
            }
            
            return client_info
            
        except Exception as e:
            self.logger.error(f"Failed to get client info: {e}")
            return {}
    
    def get_stats_info(self) -> Dict:
        """Get statistics information"""
        try:
            info = self.redis_client.info('stats')
            
            stats_info = {
                'total_connections_received': info.get('total_connections_received', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'instantaneous_ops_per_sec': info.get('instantaneous_ops_per_sec', 0),
                'total_net_input_bytes': info.get('total_net_input_bytes', 0),
                'total_net_output_bytes': info.get('total_net_output_bytes', 0),
                'rejected_connections': info.get('rejected_connections', 0),
                'expired_keys': info.get('expired_keys', 0),
                'evicted_keys': info.get('evicted_keys', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'pubsub_channels': info.get('pubsub_channels', 0),
                'pubsub_patterns': info.get('pubsub_patterns', 0)
            }
            
            # Calculate hit ratio
            total_requests = stats_info['keyspace_hits'] + stats_info['keyspace_misses']
            if total_requests > 0:
                stats_info['hit_ratio_percent'] = round(
                    (stats_info['keyspace_hits'] / total_requests) * 100, 2
                )
            else:
                stats_info['hit_ratio_percent'] = 0.0
            
            return stats_info
            
        except Exception as e:
            self.logger.error(f"Failed to get stats info: {e}")
            return {}
    
    def get_keyspace_info(self) -> Dict:
        """Get keyspace information"""
        try:
            info = self.redis_client.info('keyspace')
            
            keyspace_info = {}
            for key, value in info.items():
                if key.startswith('db'):
                    db_num = key
                    # Parse the database info string
                    # Format: "keys=X,expires=Y,avg_ttl=Z"
                    db_info = {}
                    for pair in value.split(','):
                        k, v = pair.split('=')
                        db_info[k] = int(v)
                    keyspace_info[db_num] = db_info
            
            return keyspace_info
            
        except Exception as e:
            self.logger.error(f"Failed to get keyspace info: {e}")
            return {}
    
    def get_replication_info(self) -> Dict:
        """Get replication information"""
        try:
            info = self.redis_client.info('replication')
            
            replication_info = {
                'role': info.get('role', 'unknown'),
                'connected_slaves': info.get('connected_slaves', 0),
                'master_replid': info.get('master_replid', 'Unknown'),
                'master_repl_offset': info.get('master_repl_offset', 0),
                'repl_backlog_active': info.get('repl_backlog_active', 0),
                'repl_backlog_size': info.get('repl_backlog_size', 0),
                'repl_backlog_first_byte_offset': info.get('repl_backlog_first_byte_offset', 0),
                'repl_backlog_histlen': info.get('repl_backlog_histlen', 0)
            }
            
            # If this is a slave, get master info
            if replication_info['role'] == 'slave':
                replication_info.update({
                    'master_host': info.get('master_host', 'Unknown'),
                    'master_port': info.get('master_port', 'Unknown'),
                    'master_link_status': info.get('master_link_status', 'Unknown'),
                    'master_last_io_seconds_ago': info.get('master_last_io_seconds_ago', 0),
                    'master_sync_in_progress': info.get('master_sync_in_progress', 0),
                    'slave_repl_offset': info.get('slave_repl_offset', 0),
                    'slave_priority': info.get('slave_priority', 100),
                    'slave_read_only': info.get('slave_read_only', 1)
                })
            
            return replication_info
            
        except Exception as e:
            self.logger.error(f"Failed to get replication info: {e}")
            return {}
    
    def get_persistence_info(self) -> Dict:
        """Get persistence information"""
        try:
            info = self.redis_client.info('persistence')
            
            persistence_info = {
                'loading': info.get('loading', 0),
                'rdb_changes_since_last_save': info.get('rdb_changes_since_last_save', 0),
                'rdb_bgsave_in_progress': info.get('rdb_bgsave_in_progress', 0),
                'rdb_last_save_time': info.get('rdb_last_save_time', 0),
                'rdb_last_bgsave_status': info.get('rdb_last_bgsave_status', 'ok'),
                'rdb_last_bgsave_time_sec': info.get('rdb_last_bgsave_time_sec', -1),
                'rdb_current_bgsave_time_sec': info.get('rdb_current_bgsave_time_sec', -1),
                'aof_enabled': info.get('aof_enabled', 0),
                'aof_rewrite_in_progress': info.get('aof_rewrite_in_progress', 0),
                'aof_rewrite_scheduled': info.get('aof_rewrite_scheduled', 0),
                'aof_last_rewrite_time_sec': info.get('aof_last_rewrite_time_sec', -1),
                'aof_current_rewrite_time_sec': info.get('aof_current_rewrite_time_sec', -1),
                'aof_last_bgrewrite_status': info.get('aof_last_bgrewrite_status', 'ok'),
                'aof_last_write_status': info.get('aof_last_write_status', 'ok')
            }
            
            # Convert timestamps to human readable format
            if persistence_info['rdb_last_save_time'] > 0:
                persistence_info['rdb_last_save_time_human'] = datetime.fromtimestamp(
                    persistence_info['rdb_last_save_time']
                ).strftime('%Y-%m-%d %H:%M:%S')
            else:
                persistence_info['rdb_last_save_time_human'] = 'Never'
            
            return persistence_info
            
        except Exception as e:
            self.logger.error(f"Failed to get persistence info: {e}")
            return {}
    
    def get_slow_queries(self, count: int = 10) -> List[Dict]:
        """Get slow queries from Redis"""
        try:
            slow_log = self.redis_client.slowlog_get(count)
            
            slow_queries = []
            for entry in slow_log:
                query_info = {
                    'id': entry['id'],
                    'timestamp': datetime.fromtimestamp(entry['start_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    'duration_microseconds': entry['duration'],
                    'duration_milliseconds': round(entry['duration'] / 1000, 2),
                    'command': ' '.join(str(arg) for arg in entry['command']),
                    'client_address': entry.get('client_address', 'Unknown'),
                    'client_name': entry.get('client_name', 'Unknown')
                }
                slow_queries.append(query_info)
            
            return slow_queries
            
        except Exception as e:
            self.logger.error(f"Failed to get slow queries: {e}")
            return []
    
    def get_cluster_info(self) -> Optional[Dict]:
        """Get cluster information if in cluster mode"""
        if not self.cluster_mode:
            return None
        
        try:
            cluster_info = self.redis_client.cluster_info()
            cluster_nodes = self.redis_client.cluster_nodes()
            
            cluster_data = {
                'cluster_state': cluster_info.get('cluster_state', 'unknown'),
                'cluster_slots_assigned': cluster_info.get('cluster_slots_assigned', 0),
                'cluster_slots_ok': cluster_info.get('cluster_slots_ok', 0),
                'cluster_slots_pfail': cluster_info.get('cluster_slots_pfail', 0),
                'cluster_slots_fail': cluster_info.get('cluster_slots_fail', 0),
                'cluster_known_nodes': cluster_info.get('cluster_known_nodes', 0),
                'cluster_size': cluster_info.get('cluster_size', 0),
                'cluster_current_epoch': cluster_info.get('cluster_current_epoch', 0),
                'nodes': []
            }
            
            # Parse cluster nodes information
            for node in cluster_nodes:
                node_info = {
                    'id': node.get('id', 'Unknown'),
                    'host': node.get('host', 'Unknown'),
                    'port': node.get('port', 0),
                    'flags': node.get('flags', []),
                    'master': node.get('master', 'Unknown'),
                    'slots': node.get('slots', []),
                    'migrations': node.get('migrations', {})
                }
                cluster_data['nodes'].append(node_info)
            
            return cluster_data
            
        except Exception as e:
            self.logger.error(f"Failed to get cluster info: {e}")
            return None
    
    def analyze_key_patterns(self, sample_size: int = 1000) -> Dict:
        """Analyze key patterns and sizes"""
        try:
            self.logger.info(f"Analyzing key patterns (sampling {sample_size} keys)...")
            
            patterns = {}
            total_size = 0
            key_count = 0
            
            # Get a sample of keys
            for key in self.redis_client.scan_iter(count=sample_size):
                if key_count >= sample_size:
                    break
                
                try:
                    key_type = self.redis_client.type(key)
                    key_size = self.redis_client.memory_usage(key) or 0
                    
                    # Extract pattern (first part before : or _ if exists)
                    pattern = key.split(':')[0].split('_')[0] if ':' in key or '_' in key else 'simple'
                    
                    if pattern not in patterns:
                        patterns[pattern] = {
                            'count': 0,
                            'total_size': 0,
                            'types': {},
                            'avg_size': 0,
                            'max_size': 0,
                            'sample_keys': []
                        }
                    
                    patterns[pattern]['count'] += 1
                    patterns[pattern]['total_size'] += key_size
                    patterns[pattern]['types'][key_type] = patterns[pattern]['types'].get(key_type, 0) + 1
                    patterns[pattern]['max_size'] = max(patterns[pattern]['max_size'], key_size)
                    
                    if len(patterns[pattern]['sample_keys']) < 5:
                        patterns[pattern]['sample_keys'].append(key)
                    
                    total_size += key_size
                    key_count += 1
                    
                except Exception as e:
                    self.logger.debug(f"Error analyzing key {key}: {e}")
                    continue
            
            # Calculate averages
            for pattern in patterns:
                if patterns[pattern]['count'] > 0:
                    patterns[pattern]['avg_size'] = patterns[pattern]['total_size'] / patterns[pattern]['count']
            
            analysis = {
                'total_keys_analyzed': key_count,
                'total_memory_analyzed': total_size,
                'patterns': patterns,
                'top_patterns': sorted(patterns.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Failed to analyze key patterns: {e}")
            return {}
    
    def get_config(self) -> Dict:
        """Get Redis configuration"""
        try:
            config = self.redis_client.config_get()
            
            # Focus on important configuration parameters
            important_configs = [
                'maxmemory', 'maxmemory-policy', 'save', 'appendonly', 
                'appendfsync', 'timeout', 'tcp-keepalive', 'maxclients',
                'databases', 'port', 'bind', 'protected-mode'
            ]
            
            filtered_config = {k: config.get(k, 'Not Set') for k in important_configs if k in config}
            
            return {
                'important_settings': filtered_config,
                'all_settings': config
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get configuration: {e}")
            return {}
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate comprehensive Redis monitoring report"""
        self.logger.info("Generating comprehensive Redis monitoring report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'server_info': self.get_server_info(),
            'memory_info': self.get_memory_info(),
            'client_info': self.get_client_info(),
            'stats_info': self.get_stats_info(),
            'keyspace_info': self.get_keyspace_info(),
            'replication_info': self.get_replication_info(),
            'persistence_info': self.get_persistence_info(),
            'slow_queries': self.get_slow_queries(),
            'configuration': self.get_config(),
            'key_analysis': self.analyze_key_patterns(),
            'cluster_info': self.get_cluster_info() if self.cluster_mode else None,
            'health_checks': self.perform_health_checks()
        }
        
        return report
    
    def perform_health_checks(self) -> List[Dict]:
        """Perform health checks and return recommendations"""
        checks = []
        
        try:
            memory_info = self.get_memory_info()
            stats_info = self.get_stats_info()
            
            # Memory usage check
            if memory_info.get('memory_usage_percent', 0) > 90:
                checks.append({
                    'type': 'CRITICAL',
                    'category': 'Memory',
                    'message': f"Memory usage is {memory_info['memory_usage_percent']:.1f}% - critically high",
                    'recommendation': 'Consider increasing maxmemory or implementing eviction policy'
                })
            elif memory_info.get('memory_usage_percent', 0) > 75:
                checks.append({
                    'type': 'WARNING',
                    'category': 'Memory',
                    'message': f"Memory usage is {memory_info['memory_usage_percent']:.1f}% - monitor closely",
                    'recommendation': 'Plan for memory optimization or scaling'
                })
            
            # Fragmentation ratio check
            if memory_info.get('mem_fragmentation_ratio', 0) > 1.5:
                checks.append({
                    'type': 'WARNING',
                    'category': 'Memory',
                    'message': f"Memory fragmentation ratio is {memory_info['mem_fragmentation_ratio']:.2f}",
                    'recommendation': 'Consider restarting Redis or using memory defragmentation'
                })
            
            # Hit ratio check
            if stats_info.get('hit_ratio_percent', 100) < 80:
                checks.append({
                    'type': 'WARNING',
                    'category': 'Performance',
                    'message': f"Cache hit ratio is {stats_info['hit_ratio_percent']:.1f}% - low efficiency",
                    'recommendation': 'Review caching strategy and TTL settings'
                })
            
            # Evicted keys check
            if stats_info.get('evicted_keys', 0) > 0:
                checks.append({
                    'type': 'INFO',
                    'category': 'Memory',
                    'message': f"{stats_info['evicted_keys']} keys have been evicted",
                    'recommendation': 'Monitor memory usage and consider increasing maxmemory'
                })
            
            # Rejected connections check
            if stats_info.get('rejected_connections', 0) > 0:
                checks.append({
                    'type': 'WARNING',
                    'category': 'Connections',
                    'message': f"{stats_info['rejected_connections']} connections have been rejected",
                    'recommendation': 'Check maxclients setting and connection pool configuration'
                })
            
            if not checks:
                checks.append({
                    'type': 'INFO',
                    'category': 'General',
                    'message': 'All health checks passed',
                    'recommendation': 'Continue monitoring regularly'
                })
            
        except Exception as e:
            checks.append({
                'type': 'ERROR',
                'category': 'Monitoring',
                'message': f'Health check failed: {str(e)}',
                'recommendation': 'Check Redis connectivity and permissions'
            })
        
        return checks
    
    def print_report_summary(self, report: Dict):
        """Print a formatted summary of the report"""
        print("\n" + "="*70)
        print("REDIS MONITORING REPORT")
        print("="*70)
        
        server_info = report['server_info']
        memory_info = report['memory_info']
        stats_info = report['stats_info']
        
        print(f"Server: {self.host}:{self.port}")
        print(f"Redis Version: {server_info.get('redis_version', 'Unknown')}")
        print(f"Mode: {server_info.get('redis_mode', 'standalone')}")
        print(f"Uptime: {server_info.get('uptime_in_days', 0)} days")
        print(f"Memory Usage: {memory_info.get('used_memory_human', '0B')} / {memory_info.get('maxmemory_human', 'No limit')}")
        print(f"Memory Usage %: {memory_info.get('memory_usage_percent', 0):.1f}%")
        print(f"Connected Clients: {report['client_info'].get('connected_clients', 0)}")
        print(f"Ops/sec: {stats_info.get('instantaneous_ops_per_sec', 0)}")
        print(f"Cache Hit Ratio: {stats_info.get('hit_ratio_percent', 0):.1f}%")
        
        # Keyspace info
        keyspace = report['keyspace_info']
        if keyspace:
            print(f"\nDatabases:")
            for db, info in keyspace.items():
                print(f"  {db}: {info.get('keys', 0)} keys")
        
        # Health checks
        health_checks = report['health_checks']
        if health_checks:
            print(f"\nHealth Checks:")
            for check in health_checks:
                status_color = {
                    'CRITICAL': '🔴',
                    'WARNING': '🟡', 
                    'INFO': '🟢',
                    'ERROR': '🔴'
                }.get(check['type'], '⚪')
                print(f"  {status_color} {check['type']}: {check['message']}")
        
        print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(description='Redis Monitoring and Management Tool')
    parser.add_argument('--host', default='localhost', help='Redis host')
    parser.add_argument('--port', type=int, default=6379, help='Redis port')
    parser.add_argument('--password', help='Redis password')
    parser.add_argument('--db', type=int, default=0, help='Redis database number')
    parser.add_argument('--cluster', action='store_true', help='Connect to Redis cluster')
    parser.add_argument('--output', help='Output file for JSON report')
    parser.add_argument('--action', choices=['monitor', 'slowlog', 'keys', 'health'], 
                       default='monitor', help='Action to perform')
    
    args = parser.parse_args()
    
    # Initialize Redis monitor
    monitor = RedisMonitor(
        host=args.host,
        port=args.port,
        password=args.password,
        db=args.db,
        cluster_mode=args.cluster
    )
    
    try:
        if args.action == 'monitor':
            report = monitor.generate_comprehensive_report()
            monitor.print_report_summary(report)
            
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                print(f"\nDetailed report saved to: {args.output}")
        
        elif args.action == 'slowlog':
            slow_queries = monitor.get_slow_queries(20)
            print(f"\nSlow Queries (Top 20):")
            print("-" * 80)
            for query in slow_queries:
                print(f"ID: {query['id']} | Duration: {query['duration_milliseconds']}ms | "
                      f"Time: {query['timestamp']}")
                print(f"Command: {query['command'][:100]}...")
                print()
        
        elif args.action == 'keys':
            analysis = monitor.analyze_key_patterns(2000)
            print(f"\nKey Pattern Analysis:")
            print("-" * 50)
            print(f"Total keys analyzed: {analysis.get('total_keys_analyzed', 0)}")
            print(f"Total memory analyzed: {analysis.get('total_memory_analyzed', 0)} bytes")
            print(f"\nTop Patterns:")
            for pattern, info in analysis.get('top_patterns', [])[:10]:
                print(f"  {pattern}: {info['count']} keys, avg size: {info['avg_size']:.0f} bytes")
        
        elif args.action == 'health':
            health_checks = monitor.perform_health_checks()
            print(f"\nHealth Check Results:")
            print("-" * 50)
            for check in health_checks:
                print(f"{check['type']}: {check['message']}")
                print(f"  Recommendation: {check['recommendation']}")
                print()
    
    except KeyboardInterrupt:
        print("\nMonitoring interrupted by user")
    except Exception as e:
        monitor.logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()