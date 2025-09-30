#!/usr/bin/env python3
"""
PostgreSQL Docker-to-Host Transfer Script
Enables secure transfer of PostgreSQL databases from Docker containers to the host.
"""

import os
import sys
import subprocess
import json
import getpass
from typing import List, Dict, Optional
import tempfile
from datetime import datetime
import configparser

class PostgreSQLTransfer:
    def __init__(self):
        self.postgres_containers = []
        self.selected_container = None
        self.connection_params = {}
        self.databases = []
        self.selected_database = None
        self.config = self.load_config()
    
    def load_config(self) -> configparser.ConfigParser:
        """Load configuration from config.ini"""
        config = configparser.ConfigParser()
        config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
        
        if os.path.exists(config_file):
            config.read(config_file)
        else:
            # Default values if config file doesn't exist
            config['host'] = {
                'host': 'localhost',
                'port': '5432',
                'username': 'postgres'
            }
            config['transfer'] = {
                'dump_options': '--no-owner --no-privileges',
                'timeout': '10'
            }
        
        return config
        
    def check_dependencies(self) -> bool:
        """Checks if all required programs are available"""
        # Check Docker
        try:
            subprocess.run(['docker', '--version'], capture_output=True, text=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ Docker not available")
            return False
            
        # Check PostgreSQL client
        try:
            subprocess.run(['psql', '--version'], capture_output=True, text=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ PostgreSQL client not installed")
            return False
            
        # Check pg_dump
        try:
            subprocess.run(['pg_dump', '--version'], capture_output=True, text=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ pg_dump not available")
            return False
            
        return True
    
    def check_docker_running(self) -> bool:
        """Checks if Docker is running"""
        try:
            subprocess.run(['docker', 'info'], capture_output=True, text=True, check=True)
            return True
        except subprocess.CalledProcessError as e:
            print("❌ Docker not running or permission denied")
            print("Try: sudo python3 postgres_transfer.py")
            print("Or add user to docker group: sudo usermod -aG docker $USER")
            return False
    
    def find_postgres_containers(self) -> List[Dict]:
        """Finds all running PostgreSQL Docker containers"""
        try:
            # Get all running containers in JSON format (easier to parse)
            result = subprocess.run([
                'docker', 'ps', '--format', 'json'
            ], capture_output=True, text=True, check=True)
            
            containers = []
            
            # Each line is a separate JSON object
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    try:
                        container_data = json.loads(line)
                        container_id = container_data.get('ID', '')
                        name = container_data.get('Names', '')
                        image = container_data.get('Image', '')
                        status = container_data.get('Status', '')
                        ports = container_data.get('Ports', '')
                        
                        # Identify PostgreSQL containers - only real PostgreSQL servers
                        if self._is_postgres_database_server(image, ports):
                            # Get additional container details
                            inspect_result = subprocess.run([
                                'docker', 'inspect', container_id
                            ], capture_output=True, text=True, check=True)
                            
                            inspect_data = json.loads(inspect_result.stdout)[0]
                            
                            container_info = {
                                'id': container_id,
                                'name': name,
                                'image': image,
                                'status': status,
                                'ports': ports,
                                'env_vars': inspect_data.get('Config', {}).get('Env', [])
                            }
                            containers.append(container_info)
                        elif any(keyword in image.lower() for keyword in ['postgresql']):
                            pass  # Skip applications
                            
                    except json.JSONDecodeError:
                        continue
            
            # Fallback: If JSON format doesn't work, use standard format
            if not containers and result.stdout.strip():
                return self._find_postgres_containers_fallback()
            
            self.postgres_containers = containers
            
            if not containers:
                print("❌ No PostgreSQL containers found")
                
                # Check if any containers are running at all
                if result.stdout.strip():
                    print("Running containers found, but none are recognized as PostgreSQL servers")
                    print("Supported images: postgres, postgres:*, postgis/postgis, timescale/timescaledb, bitnami/postgresql")
                    print("Check your container images with: docker ps")
                else:
                    print("No containers are currently running")
                    print("Start your PostgreSQL container first")
            
            return containers
                
        except subprocess.CalledProcessError as e:
            print(f"❌ Error searching for containers: {e}")
            return []
    
    def _find_postgres_containers_fallback(self) -> List[Dict]:
        """Fallback method for container detection with standard docker ps"""
        try:
            # Standard docker ps without special formatting
            result = subprocess.run([
                'docker', 'ps'
            ], capture_output=True, text=True, check=True)
            
            containers = []
            lines = result.stdout.strip().split('\n')[1:]  # Skip header
            
            for line in lines:
                if line.strip():
                    # Parse standard docker ps format
                    # CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES
                    parts = line.split()
                    if len(parts) >= 7:  # At least 7 parts expected
                        container_id = parts[0]
                        image = parts[1]
                        
                        # Name is always the last element
                        name = parts[-1]
                        
                        # Status and ports can contain multiple words
                        # Simplified extraction
                        status_start = 4
                        ports_and_names = ' '.join(parts[status_start:])
                        
                        # Identify PostgreSQL containers - only real PostgreSQL servers
                        if self._is_postgres_database_server(image, ''):
                            
                            # Get additional container details
                            inspect_result = subprocess.run([
                                'docker', 'inspect', container_id
                            ], capture_output=True, text=True, check=True)
                            
                            inspect_data = json.loads(inspect_result.stdout)[0]
                            
                            container_info = {
                                'id': container_id,
                                'name': name,
                                'image': image,
                                'status': 'running',  # Simplified, since all ps containers are running
                                'ports': '',  # Will be extracted from inspect_data later if needed
                                'env_vars': inspect_data.get('Config', {}).get('Env', [])
                            }
                            containers.append(container_info)
                        elif any(keyword in image.lower() for keyword in ['postgresql']):
                            print(f"  ⚠️  Skipping application with PostgreSQL: {name} ({image})")
            
            return containers
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error in fallback method: {e}")
            return []
    
    def _is_postgres_database_server(self, image: str, ports: str) -> bool:
        """Checks if this is a real PostgreSQL database server"""
        image_lower = image.lower()
        
        # Recognize real PostgreSQL images
        postgres_server_patterns = [
            'postgres:',           # Official PostgreSQL image with tag
            'postgres',            # Official PostgreSQL image without tag
            'postgresql:',         # Alternative PostgreSQL images
            'postgresql',          # Alternative PostgreSQL images without tag
            'postgres/',           # PostgreSQL with namespace
            'bitnami/postgresql',  # Bitnami PostgreSQL
            'postgis/postgis',     # PostGIS (based on PostgreSQL)
            'timescale/timescaledb' # TimescaleDB (based on PostgreSQL)
        ]
        
        # Check if it's a real PostgreSQL server image
        is_postgres_server = any(
            pattern in image_lower or image_lower == pattern.rstrip(':') 
            for pattern in postgres_server_patterns
        )
        
        # Exclude applications that only use PostgreSQL
        exclude_patterns = [
            'umami',              # Umami Analytics
            'nextcloud',          # Nextcloud
            'wordpress',          # WordPress
            'drupal',             # Drupal
            'gitlab',             # GitLab
            'sonarqube',          # SonarQube
            'keycloak',           # Keycloak
            'superset',           # Apache Superset
            'metabase',           # Metabase
            'grafana',            # Grafana
            'hasura',             # Hasura
            'supabase',           # Supabase
            'directus',           # Directus
            'strapi'              # Strapi
        ]
        
        is_application = any(pattern in image_lower for pattern in exclude_patterns)
        
        # Additional check: PostgreSQL port 5432 should be exposed
        has_postgres_port = '5432' in ports if ports else True  # True if ports empty (will be checked later)
        
        # Additional check for special cases
        if 'postgresql-latest' in image_lower:
            # This is probably an application, not the DB server
            return False
        
        return is_postgres_server and not is_application and has_postgres_port
    
    def display_containers(self) -> bool:
        """Displays available containers and lets user select one"""
        if not self.postgres_containers:
            return False
            
        BOLD = '\033[1m'
        RESET = '\033[0m'
        print(f"{BOLD}PostgreSQL containers:{RESET}")
        for i, container in enumerate(self.postgres_containers, 1):
            print(f"{i}) {container['name']} ({container['image']})")
        
        while True:
            try:
                choice = input(f"Select a container (1-{len(self.postgres_containers)}) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    print("Cancelled.")
                    return False
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(self.postgres_containers):
                    self.selected_container = self.postgres_containers[choice_num - 1]
                    return True
                else:
                    print("Invalid selection")
                    
            except ValueError:
                print("❌ Please enter a number.")
    
    def get_connection_details(self) -> bool:
        """Gets database connection credentials"""
        # Derive default values from container environment variables
        env_vars = self.selected_container.get('env_vars', [])
        default_user = 'postgres'
        default_port = '5432'
        
        for env_var in env_vars:
            if env_var.startswith('POSTGRES_USER='):
                default_user = env_var.split('=', 1)[1]
            elif env_var.startswith('PGPORT='):
                default_port = env_var.split('=', 1)[1]
        
        # User inputs
        BOLD = '\033[1m'
        RESET = '\033[0m'
        print()
        print(f"{BOLD}Connection details for {self.selected_container['name']}:{RESET}")
        host = input(f"Host (default: localhost): ").strip() or 'localhost'
        port = input(f"Port (default: {default_port}): ").strip() or default_port
        username = input(f"Username (default: {default_user}): ").strip() or default_user
        password = getpass.getpass("Password: ")
        
        self.connection_params = {
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'container_id': self.selected_container['id']
        }
        
        return True
    
    def test_connection(self) -> bool:
        """Tests the database connection"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = self.connection_params['password']
            
            result = subprocess.run([
                'docker', 'exec', '-i', self.selected_container['id'],
                'psql', 
                '-h', self.connection_params['host'],
                '-p', self.connection_params['port'],
                '-U', self.connection_params['username'],
                '-d', 'postgres',
                '-c', 'SELECT version();'
            ], env=env, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                return True
            else:
                print(f"Connection failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("Connection timeout")
            return False
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def list_databases(self) -> bool:
        """Lists all available databases"""
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = self.connection_params['password']
            
            result = subprocess.run([
                'docker', 'exec', '-i', self.selected_container['id'],
                'psql',
                '-h', self.connection_params['host'],
                '-p', self.connection_params['port'],
                '-U', self.connection_params['username'],
                '-d', 'postgres',
                '-t', '-c',
                "SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres';"
            ], env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                databases = [db.strip() for db in result.stdout.strip().split('\n') if db.strip()]
                
                # Add postgres database if not present
                if 'postgres' not in databases:
                    databases.insert(0, 'postgres')
                
                self.databases = databases
                
                if not databases:
                    print("No databases found")
                    return False
                return True
            else:
                print(f"Error loading databases: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"Error loading databases: {e}")
            return False
    
    def select_database(self) -> bool:
        """Lets user select a database"""
        if not self.databases:
            return False
            
        BOLD = '\033[1m'
        RESET = '\033[0m'
        print()
        print(f"{BOLD}Available databases:{RESET}")
        for i, db in enumerate(self.databases, 1):
            print(f"{i}) {db}")
        
        while True:
            try:
                choice = input(f"Select a database (1-{len(self.databases)}) or 'q' to quit: ").strip()
                
                if choice.lower() == 'q':
                    print("Cancelled.")
                    return False
                
                choice_num = int(choice)
                if 1 <= choice_num <= len(self.databases):
                    self.selected_database = self.databases[choice_num - 1]
                    return True
                else:
                    print("Invalid selection")
                    
            except ValueError:
                print("❌ Please enter a number.")
    
    def show_transfer_overview(self) -> bool:
        """Shows overview of the planned transfer"""
        BOLD = '\033[1m'
        RESET = '\033[0m'
        print()
        print(f"{BOLD}Transfer: {self.selected_database} from {self.selected_container['name']}{RESET}")
        
        while True:
            confirm = input("Start transfer? (y/n): ").strip().lower()
            if confirm in ['y', 'yes']:
                return True
            elif confirm in ['n', 'no']:
                return False
            else:
                print("Please answer y or n")
    
    def get_host_postgres_details(self) -> Optional[Dict]:
        """Gets host PostgreSQL connection details from config and user input"""
        # Get settings from config
        host_host = self.config.get('host', 'host', fallback='localhost')
        host_port = self.config.get('host', 'port', fallback='5432')
        host_username = self.config.get('host', 'username', fallback='postgres')
        
        # Only ask for password
        host_password = getpass.getpass(f"Host PostgreSQL password for {host_username}@{host_host}: ")
        
        # Test host connection
        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = host_password
            
            result = subprocess.run([
                'psql',
                '-h', host_host,
                '-p', host_port,
                '-U', host_username,
                '-d', 'postgres',
                '-c', 'SELECT version();'
            ], env=env, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                return {
                    'host': host_host,
                    'port': host_port,
                    'username': host_username,
                    'password': host_password
                }
            else:
                print(f"Host connection failed: {result.stderr}")
                return None
                
        except Exception as e:
            print(f"Host connection error: {e}")
            return None
    
    def perform_transfer(self) -> bool:
        """Performs the actual database transfer"""
        # Get host PostgreSQL details
        host_details = self.get_host_postgres_details()
        if not host_details:
            print("Host connection failed")
            return False
        
        # Create temporary dump file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dump_filename = f"{self.selected_database}_dump_{timestamp}.sql"
        dump_path = os.path.join(tempfile.gettempdir(), dump_filename)
        
        try:
            # Step 1: Dump database from container
            print("Exporting...")
            
            env = os.environ.copy()
            env['PGPASSWORD'] = self.connection_params['password']
            
            dump_options = self.config.get('transfer', 'dump_options', fallback='--no-owner --no-privileges').split()
            
            dump_result = subprocess.run([
                'docker', 'exec', '-i', self.selected_container['id'],
                'pg_dump',
                '-h', self.connection_params['host'],
                '-p', self.connection_params['port'],
                '-U', self.connection_params['username'],
                '-d', self.selected_database
            ] + dump_options, env=env, capture_output=True, text=True)
            
            if dump_result.returncode != 0:
                print(f"Export failed: {dump_result.stderr}")
                return False
            
            # Write dump to file
            with open(dump_path, 'w', encoding='utf-8') as f:
                f.write(dump_result.stdout)
            
            # Step 2: Create/check target database on host
            print("Preparing target...")
            
            host_env = os.environ.copy()
            host_env['PGPASSWORD'] = host_details['password']
            
            # Check if database already exists
            check_result = subprocess.run([
                'psql',
                '-h', host_details['host'],
                '-p', host_details['port'],
                '-U', host_details['username'],
                '-d', 'postgres',
                '-t', '-c',
                f"SELECT 1 FROM pg_database WHERE datname='{self.selected_database}';"
            ], env=host_env, capture_output=True, text=True)
            
            if check_result.returncode == 0 and check_result.stdout.strip():
                # Red color ANSI codes
                RED = '\033[91m'
                BOLD = '\033[1m'
                RESET = '\033[0m'
                
                print(f"\n{RED}{BOLD}WARNING: Database '{self.selected_database}' already exists!{RESET}")
                print(f"{RED}This will PERMANENTLY DELETE all existing data!{RESET}")
                overwrite = input(f"{RED}Overwrite '{self.selected_database}'?{RESET} (y/n): ").strip().lower()
                
                if overwrite not in ['y', 'yes']:
                    return False
                
                # Drop database
                drop_result = subprocess.run([
                    'psql',
                    '-h', host_details['host'],
                    '-p', host_details['port'],
                    '-U', host_details['username'],
                    '-d', 'postgres',
                    '-c', f'DROP DATABASE "{self.selected_database}";'
                ], env=host_env, capture_output=True, text=True)
                
                if drop_result.returncode != 0:
                    print(f"Error dropping database: {drop_result.stderr}")
                    return False
            
            # Create new database
            create_result = subprocess.run([
                'psql',
                '-h', host_details['host'],
                '-p', host_details['port'],
                '-U', host_details['username'],
                '-d', 'postgres',
                '-c', f'CREATE DATABASE "{self.selected_database}";'
            ], env=host_env, capture_output=True, text=True)
            
            if create_result.returncode != 0:
                print(f"Error creating database: {create_result.stderr}")
                return False
            
            # Step 3: Import dump into host database
            print("Importing...")
            
            with open(dump_path, 'r', encoding='utf-8') as f:
                restore_result = subprocess.run([
                    'psql',
                    '-h', host_details['host'],
                    '-p', host_details['port'],
                    '-U', host_details['username'],
                    '-d', self.selected_database
                ], env=host_env, stdin=f, capture_output=True, text=True)
            
            if restore_result.returncode != 0:
                print(f"Import completed with warnings: {restore_result.stderr}")
            
            # Success message in green and bold
            GREEN = '\033[92m'
            BOLD = '\033[1m'
            RESET = '\033[0m'
            print()
            print(f"{GREEN}{BOLD}Transfer completed: {self.selected_database}{RESET}")
            return True
            
        except Exception as e:
            print(f"Transfer error: {e}")
            return False
        finally:
            # Cleanup - always executed, even on errors or keyboard interrupt
            if os.path.exists(dump_path):
                os.remove(dump_path)
    
    def run(self) -> None:
        """Main program loop"""
        print("PostgreSQL Docker-to-Host Transfer")
        print()
        
        # Check dependencies
        print("Checking dependencies...")
        if not self.check_dependencies():
            sys.exit(1)
        
        # Check Docker status
        print("Checking Docker status...")
        if not self.check_docker_running():
            sys.exit(1)
        
        # Find PostgreSQL containers
        print("Searching for PostgreSQL containers...")
        if not self.find_postgres_containers():
            sys.exit(1)
        
        # Select container
        if not self.display_containers():
            sys.exit(1)
        
        # Get connection details
        if not self.get_connection_details():
            sys.exit(1)
        
        # Test connection
        if not self.test_connection():
            sys.exit(1)
        
        # List databases
        if not self.list_databases():
            sys.exit(1)
        
        # Select database
        if not self.select_database():
            sys.exit(1)
        
        # Show overview and confirm
        if not self.show_transfer_overview():
            sys.exit(1)
        
        # Perform transfer
        if not self.perform_transfer():
            sys.exit(1)


def main():
    """Main function"""
    try:
        transfer = PostgreSQLTransfer()
        transfer.run()
    except KeyboardInterrupt:
        print("\nCancelled")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()