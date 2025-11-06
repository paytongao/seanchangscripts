#!/usr/bin/env python3
"""
Airtable Migration Script
Migrates startup extract JSON files from local storage to Airtable records.
"""

import os
import json
import re
from pathlib import Path
from typing import Optional, Dict, Any
from pyairtable import Table

class AirtableMigration:
    def __init__(self, api_key: str, base_id: str, table_name: str = "VC Database"):
        """
        Initialize the Airtable migration client.
        
        Args:
            api_key: Airtable API key
            base_id: Airtable base ID
            table_name: Name of the table to update (default: "VC Database")
        """
        self.airtable = Table(api_key, base_id, table_name)
        self.output_dir = Path("output/runs")
        
    def normalize_vc_name_to_folder(self, vc_name: str) -> str:
        """
        Convert VC name to folder name format.
        
        Args:
            vc_name: The VC/Investor name from Airtable
            
        Returns:
            Normalized folder name
        """
        # Convert to lowercase
        folder_name = vc_name.lower()
        
        # Replace spaces and common punctuation with underscores
        folder_name = re.sub(r'[^a-z0-9]+', '_', folder_name)
        
        # Remove leading/trailing underscores
        folder_name = folder_name.strip('_')
        
        # Handle special cases based on observed patterns
        folder_name = folder_name.replace('__', '_')
        
        return folder_name
    
    def find_startup_extract_file(self, folder_path: Path) -> Optional[Path]:
        """
        Find the startup_extract JSON file in the given folder.
        
        Args:
            folder_path: Path to the VC folder
            
        Returns:
            Path to the startup_extract file or None if not found
        """
        if not folder_path.exists() or not folder_path.is_dir():
            return None
            
        # Look for files starting with 'startup_extract'
        for file in folder_path.glob("startup_extract_*.json"):
            return file
            
        return None
    
    def read_json_file(self, file_path: Path) -> Optional[Dict[Any, Any]]:
        """
        Read and parse a JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Parsed JSON data or None if error
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return None
    
    def update_airtable_record(self, record_id: str, json_data: Dict[Any, Any]) -> bool:
        """
        Update an Airtable record with JSON data.
        
        Args:
            record_id: The Airtable record ID
            json_data: The JSON data to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert JSON data to string for storage
            json_string = json.dumps(json_data, indent=2)
            
            # Update the record
            self.airtable.update(record_id, {
                'Startup Data Enrichment JSON': json_string,
                'Enrichment JSON extracted?': True
            })
            
            return True
        except Exception as e:
            print(f"Error updating record {record_id}: {e}")
            return False
    
    def process_all_records(self) -> None:
        """
        Process all records in the VC Database table.
        """
        try:
            # Get all records from the table
            records = self.airtable.all()
            
            print(f"Found {len(records)} records in the VC Database")
            
            processed_count = 0
            success_count = 0
            
            for record in records:
                record_id = record['id']
                fields = record['fields']
                
                # Get the VC name (assuming it's stored in a field - adjust field name as needed)
                vc_name = fields.get('VC/Investor Name')
                if not vc_name:
                    print(f"Skipping record {record_id}: No VC/Investor Name found")
                    continue
                
                # Check if already processed
                if fields.get('Enrichment JSON extracted?'):
                    print(f"Skipping {vc_name}: Already processed")
                    continue
                
                print(f"Processing: {vc_name}")
                
                # Convert VC name to folder name
                folder_name = self.normalize_vc_name_to_folder(vc_name)
                folder_path = self.output_dir / folder_name
                
                print(f"  Looking in folder: {folder_path}")
                
                # Find the startup extract file
                json_file = self.find_startup_extract_file(folder_path)
                
                if not json_file:
                    print(f"  No startup_extract file found for {vc_name}")
                    processed_count += 1
                    continue
                
                print(f"  Found file: {json_file}")
                
                # Read the JSON data
                json_data = self.read_json_file(json_file)
                
                if not json_data:
                    print(f"  Failed to read JSON data from {json_file}")
                    processed_count += 1
                    continue
                
                # Update the Airtable record
                if self.update_airtable_record(record_id, json_data):
                    print(f"  Successfully updated {vc_name}")
                    success_count += 1
                else:
                    print(f"  Failed to update {vc_name}")
                
                processed_count += 1
            
            print(f"\nMigration complete!")
            print(f"Total records processed: {processed_count}")
            print(f"Successfully updated: {success_count}")
            print(f"Failed/skipped: {processed_count - success_count}")
            
        except Exception as e:
            print(f"Error processing records: {e}")

def main():
    """
    Main function to run the migration.
    Using your Airtable credentials from workflow.py
    """
    
    # Configuration from your workflow.py file
    API_KEY = "patPnlxR05peVEnUc.e5a8cfe5a3f88676da4b3c124c99ed46026b4f869bb5b6a3f54cd45db17fd58f"
    BASE_ID = "app768aQ07mCJoyu8"
    TABLE_NAME = "VC Database"
    
    print(f"🚀 Starting migration for {TABLE_NAME}")
    print(f"📊 Base ID: {BASE_ID}")
    
    # Create and run the migration
    migration = AirtableMigration(API_KEY, BASE_ID, TABLE_NAME)
    migration.process_all_records()

if __name__ == "__main__":
    main()