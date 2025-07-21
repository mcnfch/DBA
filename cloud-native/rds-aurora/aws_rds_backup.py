#!/usr/bin/env python3
"""
AWS RDS/Aurora Backup Management Script (Python)
Author: DBA Portfolio
Purpose: Comprehensive AWS RDS and Aurora backup automation and monitoring
"""

import boto3
import json
import sys
import argparse
import datetime
import logging
from typing import Dict, List, Optional
from botocore.exceptions import ClientError, NoCredentialsError

class AWSRDSBackupManager:
    def __init__(self, region: str = 'us-east-1', profile: Optional[str] = None):
        self.region = region
        self.setup_logging()
        self.setup_aws_clients(profile)
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'aws_rds_backup_{datetime.datetime.now().strftime("%Y%m%d")}.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_aws_clients(self, profile: Optional[str]):
        """Setup AWS service clients"""
        try:
            if profile:
                session = boto3.Session(profile_name=profile)
                self.rds = session.client('rds', region_name=self.region)
                self.cloudwatch = session.client('cloudwatch', region_name=self.region)
                self.sns = session.client('sns', region_name=self.region)
            else:
                self.rds = boto3.client('rds', region_name=self.region)
                self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
                self.sns = boto3.client('sns', region_name=self.region)
            
            # Test credentials
            self.rds.describe_db_instances(MaxRecords=1)
            self.logger.info(f"AWS clients initialized successfully for region: {self.region}")
            
        except NoCredentialsError:
            self.logger.error("AWS credentials not found. Configure credentials or use --profile option.")
            sys.exit(1)
        except ClientError as e:
            self.logger.error(f"Failed to initialize AWS clients: {e}")
            sys.exit(1)
    
    def get_rds_instances(self, instance_id: Optional[str] = None) -> List[Dict]:
        """Get RDS instances"""
        try:
            if instance_id:
                response = self.rds.describe_db_instances(DBInstanceIdentifier=instance_id)
            else:
                response = self.rds.describe_db_instances()
            
            instances = []
            for db in response['DBInstances']:
                instance_info = {
                    'DBInstanceIdentifier': db['DBInstanceIdentifier'],
                    'DBInstanceClass': db['DBInstanceClass'],
                    'Engine': db['Engine'],
                    'EngineVersion': db['EngineVersion'],
                    'DBInstanceStatus': db['DBInstanceStatus'],
                    'AllocatedStorage': db.get('AllocatedStorage', 0),
                    'StorageType': db.get('StorageType', 'Unknown'),
                    'MultiAZ': db.get('MultiAZ', False),
                    'BackupRetentionPeriod': db.get('BackupRetentionPeriod', 0),
                    'PreferredBackupWindow': db.get('PreferredBackupWindow', 'Unknown'),
                    'LatestRestorableTime': db.get('LatestRestorableTime'),
                    'DeletionProtection': db.get('DeletionProtection', False),
                    'StorageEncrypted': db.get('StorageEncrypted', False)
                }
                
                # Check if it's part of Aurora cluster
                if db.get('DBClusterIdentifier'):
                    instance_info['DBClusterIdentifier'] = db['DBClusterIdentifier']
                
                instances.append(instance_info)
            
            return instances
            
        except ClientError as e:
            self.logger.error(f"Failed to get RDS instances: {e}")
            return []
    
    def get_aurora_clusters(self, cluster_id: Optional[str] = None) -> List[Dict]:
        """Get Aurora clusters"""
        try:
            if cluster_id:
                response = self.rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
            else:
                response = self.rds.describe_db_clusters()
            
            clusters = []
            for cluster in response['DBClusters']:
                cluster_info = {
                    'DBClusterIdentifier': cluster['DBClusterIdentifier'],
                    'Engine': cluster['Engine'],
                    'EngineVersion': cluster['EngineVersion'],
                    'Status': cluster['Status'],
                    'DatabaseName': cluster.get('DatabaseName', 'Unknown'),
                    'BackupRetentionPeriod': cluster.get('BackupRetentionPeriod', 0),
                    'PreferredBackupWindow': cluster.get('PreferredBackupWindow', 'Unknown'),
                    'StorageEncrypted': cluster.get('StorageEncrypted', False),
                    'DeletionProtection': cluster.get('DeletionProtection', False),
                    'ClusterMembers': [member['DBInstanceIdentifier'] for member in cluster.get('DBClusterMembers', [])]
                }
                clusters.append(cluster_info)
            
            return clusters
            
        except ClientError as e:
            self.logger.error(f"Failed to get Aurora clusters: {e}")
            return []
    
    def create_manual_snapshot(self, identifier: str, snapshot_id: str, is_cluster: bool = False) -> bool:
        """Create manual snapshot"""
        try:
            if is_cluster:
                self.logger.info(f"Creating manual snapshot for Aurora cluster: {identifier}")
                response = self.rds.create_db_cluster_snapshot(
                    DBClusterSnapshotIdentifier=snapshot_id,
                    DBClusterIdentifier=identifier
                )
                snapshot_arn = response['DBClusterSnapshot']['DBClusterSnapshotArn']
            else:
                self.logger.info(f"Creating manual snapshot for RDS instance: {identifier}")
                response = self.rds.create_db_snapshot(
                    DBSnapshotIdentifier=snapshot_id,
                    DBInstanceIdentifier=identifier
                )
                snapshot_arn = response['DBSnapshot']['DBSnapshotArn']
            
            self.logger.info(f"Snapshot creation initiated: {snapshot_arn}")
            
            # Wait for snapshot to complete
            if self.wait_for_snapshot_completion(snapshot_id, is_cluster):
                self.logger.info(f"Snapshot completed successfully: {snapshot_id}")
                return True
            else:
                self.logger.error(f"Snapshot creation failed or timed out: {snapshot_id}")
                return False
                
        except ClientError as e:
            self.logger.error(f"Failed to create snapshot: {e}")
            return False
    
    def wait_for_snapshot_completion(self, snapshot_id: str, is_cluster: bool = False, timeout: int = 3600) -> bool:
        """Wait for snapshot to complete"""
        import time
        
        start_time = time.time()
        self.logger.info(f"Waiting for snapshot completion: {snapshot_id}")
        
        while time.time() - start_time < timeout:
            try:
                if is_cluster:
                    response = self.rds.describe_db_cluster_snapshots(
                        DBClusterSnapshotIdentifier=snapshot_id
                    )
                    status = response['DBClusterSnapshots'][0]['Status']
                else:
                    response = self.rds.describe_db_snapshots(
                        DBSnapshotIdentifier=snapshot_id
                    )
                    status = response['DBSnapshots'][0]['Status']
                
                self.logger.info(f"Snapshot {snapshot_id} status: {status}")
                
                if status == 'available':
                    return True
                elif status in ['failed', 'deleted']:
                    return False
                
                time.sleep(60)  # Wait 1 minute before checking again
                
            except ClientError as e:
                self.logger.error(f"Error checking snapshot status: {e}")
                return False
        
        self.logger.error(f"Snapshot creation timed out: {snapshot_id}")
        return False
    
    def list_snapshots(self, identifier: Optional[str] = None, is_cluster: bool = False) -> List[Dict]:
        """List snapshots"""
        try:
            snapshots = []
            
            if is_cluster:
                if identifier:
                    response = self.rds.describe_db_cluster_snapshots(
                        DBClusterIdentifier=identifier,
                        SnapshotType='manual'
                    )
                else:
                    response = self.rds.describe_db_cluster_snapshots(SnapshotType='manual')
                
                for snapshot in response['DBClusterSnapshots']:
                    snapshot_info = {
                        'SnapshotId': snapshot['DBClusterSnapshotIdentifier'],
                        'SourceId': snapshot['DBClusterIdentifier'],
                        'Type': 'Cluster',
                        'Status': snapshot['Status'],
                        'SnapshotCreateTime': snapshot['SnapshotCreateTime'],
                        'Engine': snapshot['Engine'],
                        'AllocatedStorage': snapshot.get('AllocatedStorage', 0),
                        'StorageEncrypted': snapshot.get('StorageEncrypted', False),
                        'SnapshotArn': snapshot['DBClusterSnapshotArn']
                    }
                    snapshots.append(snapshot_info)
            else:
                if identifier:
                    response = self.rds.describe_db_snapshots(
                        DBInstanceIdentifier=identifier,
                        SnapshotType='manual'
                    )
                else:
                    response = self.rds.describe_db_snapshots(SnapshotType='manual')
                
                for snapshot in response['DBSnapshots']:
                    snapshot_info = {
                        'SnapshotId': snapshot['DBSnapshotIdentifier'],
                        'SourceId': snapshot['DBInstanceIdentifier'],
                        'Type': 'Instance',
                        'Status': snapshot['Status'],
                        'SnapshotCreateTime': snapshot['SnapshotCreateTime'],
                        'Engine': snapshot['Engine'],
                        'AllocatedStorage': snapshot.get('AllocatedStorage', 0),
                        'StorageEncrypted': snapshot.get('Encrypted', False),
                        'SnapshotArn': snapshot['DBSnapshotArn']
                    }
                    snapshots.append(snapshot_info)
            
            return snapshots
            
        except ClientError as e:
            self.logger.error(f"Failed to list snapshots: {e}")
            return []
    
    def delete_snapshot(self, snapshot_id: str, is_cluster: bool = False) -> bool:
        """Delete snapshot"""
        try:
            if is_cluster:
                self.logger.info(f"Deleting cluster snapshot: {snapshot_id}")
                self.rds.delete_db_cluster_snapshot(
                    DBClusterSnapshotIdentifier=snapshot_id
                )
            else:
                self.logger.info(f"Deleting instance snapshot: {snapshot_id}")
                self.rds.delete_db_snapshot(
                    DBSnapshotIdentifier=snapshot_id
                )
            
            self.logger.info(f"Snapshot deletion initiated: {snapshot_id}")
            return True
            
        except ClientError as e:
            self.logger.error(f"Failed to delete snapshot: {e}")
            return False
    
    def cleanup_old_snapshots(self, retention_days: int = 7) -> Dict[str, int]:
        """Cleanup old manual snapshots"""
        cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=retention_days)
        deleted_counts = {'instances': 0, 'clusters': 0}
        
        self.logger.info(f"Cleaning up snapshots older than {retention_days} days")
        
        # Clean up instance snapshots
        instance_snapshots = self.list_snapshots(is_cluster=False)
        for snapshot in instance_snapshots:
            if snapshot['SnapshotCreateTime'] < cutoff_date:
                if self.delete_snapshot(snapshot['SnapshotId'], is_cluster=False):
                    deleted_counts['instances'] += 1
        
        # Clean up cluster snapshots
        cluster_snapshots = self.list_snapshots(is_cluster=True)
        for snapshot in cluster_snapshots:
            if snapshot['SnapshotCreateTime'] < cutoff_date:
                if self.delete_snapshot(snapshot['SnapshotId'], is_cluster=True):
                    deleted_counts['clusters'] += 1
        
        self.logger.info(f"Cleanup completed: {deleted_counts['instances']} instance snapshots, {deleted_counts['clusters']} cluster snapshots deleted")
        return deleted_counts
    
    def modify_backup_settings(self, identifier: str, backup_retention_period: int, 
                              backup_window: Optional[str] = None, is_cluster: bool = False) -> bool:
        """Modify backup settings"""
        try:
            modify_params = {
                'BackupRetentionPeriod': backup_retention_period,
                'ApplyImmediately': True
            }
            
            if backup_window:
                modify_params['PreferredBackupWindow'] = backup_window
            
            if is_cluster:
                modify_params['DBClusterIdentifier'] = identifier
                self.logger.info(f"Modifying backup settings for Aurora cluster: {identifier}")
                self.rds.modify_db_cluster(**modify_params)
            else:
                modify_params['DBInstanceIdentifier'] = identifier
                self.logger.info(f"Modifying backup settings for RDS instance: {identifier}")
                self.rds.modify_db_instance(**modify_params)
            
            self.logger.info(f"Backup settings modified successfully for {identifier}")
            return True
            
        except ClientError as e:
            self.logger.error(f"Failed to modify backup settings: {e}")
            return False
    
    def get_backup_metrics(self, identifier: str, is_cluster: bool = False) -> Dict:
        """Get backup-related CloudWatch metrics"""
        try:
            end_time = datetime.datetime.utcnow()
            start_time = end_time - datetime.timedelta(days=1)
            
            namespace = 'AWS/RDS'
            dimension_name = 'DBClusterIdentifier' if is_cluster else 'DBInstanceIdentifier'
            
            metrics = {}
            
            # Get backup size metrics (if available)
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace=namespace,
                    MetricName='SnapshotStorageUsed',
                    Dimensions=[{'Name': dimension_name, 'Value': identifier}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Average']
                )
                
                if response['Datapoints']:
                    latest_size = max(response['Datapoints'], key=lambda x: x['Timestamp'])
                    metrics['SnapshotStorageUsed'] = latest_size['Average']
            except ClientError:
                metrics['SnapshotStorageUsed'] = None
            
            return metrics
            
        except ClientError as e:
            self.logger.error(f"Failed to get backup metrics: {e}")
            return {}
    
    def generate_backup_report(self) -> Dict:
        """Generate comprehensive backup report"""
        report = {
            'timestamp': datetime.datetime.now().isoformat(),
            'region': self.region,
            'rds_instances': [],
            'aurora_clusters': [],
            'snapshots': {
                'instances': [],
                'clusters': []
            },
            'summary': {
                'total_instances': 0,
                'total_clusters': 0,
                'total_snapshots': 0,
                'backup_issues': []
            }
        }
        
        # Get RDS instances
        instances = self.get_rds_instances()
        for instance in instances:
            instance['backup_metrics'] = self.get_backup_metrics(
                instance['DBInstanceIdentifier'], is_cluster=False
            )
            
            # Check backup configuration
            if instance['BackupRetentionPeriod'] == 0:
                report['summary']['backup_issues'].append(
                    f"Instance {instance['DBInstanceIdentifier']} has automated backups disabled"
                )
        
        report['rds_instances'] = instances
        report['summary']['total_instances'] = len(instances)
        
        # Get Aurora clusters
        clusters = self.get_aurora_clusters()
        for cluster in clusters:
            cluster['backup_metrics'] = self.get_backup_metrics(
                cluster['DBClusterIdentifier'], is_cluster=True
            )
            
            # Check backup configuration
            if cluster['BackupRetentionPeriod'] == 0:
                report['summary']['backup_issues'].append(
                    f"Cluster {cluster['DBClusterIdentifier']} has automated backups disabled"
                )
        
        report['aurora_clusters'] = clusters
        report['summary']['total_clusters'] = len(clusters)
        
        # Get snapshots
        instance_snapshots = self.list_snapshots(is_cluster=False)
        cluster_snapshots = self.list_snapshots(is_cluster=True)
        
        report['snapshots']['instances'] = instance_snapshots
        report['snapshots']['clusters'] = cluster_snapshots
        report['summary']['total_snapshots'] = len(instance_snapshots) + len(cluster_snapshots)
        
        return report
    
    def save_report(self, report: Dict, filename: str = None) -> str:
        """Save report to JSON file"""
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"aws_rds_backup_report_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"Report saved to: {filename}")
        return filename
    
    def print_summary(self, report: Dict):
        """Print backup summary to console"""
        print("\n" + "="*60)
        print("AWS RDS/Aurora Backup Summary")
        print("="*60)
        print(f"Region: {report['region']}")
        print(f"Generated: {report['timestamp']}")
        print(f"Total RDS Instances: {report['summary']['total_instances']}")
        print(f"Total Aurora Clusters: {report['summary']['total_clusters']}")
        print(f"Total Manual Snapshots: {report['summary']['total_snapshots']}")
        
        if report['summary']['backup_issues']:
            print(f"\nBackup Configuration Issues: {len(report['summary']['backup_issues'])}")
            for issue in report['summary']['backup_issues']:
                print(f"  - {issue}")
        else:
            print("\nNo backup configuration issues found")
        
        print("\nRDS Instances:")
        for instance in report['rds_instances']:
            print(f"  {instance['DBInstanceIdentifier']} ({instance['Engine']}) - "
                  f"Backup Retention: {instance['BackupRetentionPeriod']} days")
        
        print("\nAurora Clusters:")
        for cluster in report['aurora_clusters']:
            print(f"  {cluster['DBClusterIdentifier']} ({cluster['Engine']}) - "
                  f"Backup Retention: {cluster['BackupRetentionPeriod']} days")
        
        print("\nRecent Manual Snapshots:")
        all_snapshots = report['snapshots']['instances'] + report['snapshots']['clusters']
        all_snapshots.sort(key=lambda x: x['SnapshotCreateTime'], reverse=True)
        
        for snapshot in all_snapshots[:10]:  # Show last 10
            print(f"  {snapshot['SnapshotId']} ({snapshot['Type']}) - "
                  f"Created: {snapshot['SnapshotCreateTime']} - Status: {snapshot['Status']}")


def main():
    parser = argparse.ArgumentParser(description='AWS RDS/Aurora Backup Management')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--profile', help='AWS CLI profile to use')
    parser.add_argument('--action', choices=['report', 'backup', 'cleanup', 'list'], 
                       default='report', help='Action to perform')
    parser.add_argument('--identifier', help='RDS instance or Aurora cluster identifier')
    parser.add_argument('--snapshot-id', help='Snapshot identifier for backup action')
    parser.add_argument('--is-cluster', action='store_true', help='Target is Aurora cluster')
    parser.add_argument('--retention-days', type=int, default=7, 
                       help='Retention period for cleanup action')
    parser.add_argument('--output-file', help='Output file for report')
    
    args = parser.parse_args()
    
    # Initialize backup manager
    backup_manager = AWSRDSBackupManager(region=args.region, profile=args.profile)
    
    try:
        if args.action == 'report':
            # Generate comprehensive report
            report = backup_manager.generate_backup_report()
            backup_manager.print_summary(report)
            
            if args.output_file:
                backup_manager.save_report(report, args.output_file)
            else:
                backup_manager.save_report(report)
        
        elif args.action == 'backup':
            if not args.identifier or not args.snapshot_id:
                print("Error: --identifier and --snapshot-id are required for backup action")
                sys.exit(1)
            
            success = backup_manager.create_manual_snapshot(
                args.identifier, args.snapshot_id, args.is_cluster
            )
            sys.exit(0 if success else 1)
        
        elif args.action == 'cleanup':
            deleted_counts = backup_manager.cleanup_old_snapshots(args.retention_days)
            print(f"Cleanup completed: {deleted_counts}")
        
        elif args.action == 'list':
            if args.identifier:
                snapshots = backup_manager.list_snapshots(args.identifier, args.is_cluster)
            else:
                instance_snapshots = backup_manager.list_snapshots(is_cluster=False)
                cluster_snapshots = backup_manager.list_snapshots(is_cluster=True)
                snapshots = instance_snapshots + cluster_snapshots
            
            print(f"\nFound {len(snapshots)} snapshots:")
            for snapshot in snapshots:
                print(f"  {snapshot['SnapshotId']} ({snapshot['Type']}) - "
                      f"Source: {snapshot['SourceId']} - Status: {snapshot['Status']}")
    
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        backup_manager.logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()