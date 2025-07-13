import argparse
import logging
import re
import json
import urllib.parse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from datetime import datetime

class DataIngestion:
    """A helper class which contains the logic to translate the Pub/Sub message into
    a format BigQuery will accept."""
    
    def parse_method(self, message):
        """This method translates a Pub/Sub message to a dictionary which can be loaded into BigQuery.
        
        Args:
            message: A Pub/Sub message containing data in the message body
                
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from the message.
        """
        try:
            # Get the message data (bytes) and decode to string
            message_data = message.data.decode('utf-8')
            
            # Try to parse as JSON first
            try:
                data = json.loads(message_data)
                if isinstance(data, dict):
                    return self.parse_json_data(data)
                else:
                    logging.error(f"Unexpected message format: {type(data)}")
                    return self.create_error_record()
            except json.JSONDecodeError:
                # If JSON parsing fails, try URL parameter parsing
                return self.parse_url_params(message_data)
                
        except Exception as e:
            logging.error(f"Error parsing message: {e}")
            return self.create_error_record()
    
    def parse_json_data(self, data):
        """Parse JSON data into the BigQuery schema format."""
        # Get event name from various possible fields
        event_name_keys = ['event_name', 'en', 'event', 'action']
        event_name = ''
        for key in event_name_keys:
            if key in data:
                event_name = str(data[key])
                break
        
        # Get or create timestamp
        event_timestamp = self.get_event_timestamp_from_dict(data)
        
        # Create event_params from all other fields
        event_params = []
        used_keys = set(['event_name', 'en', 'event', 'action', 'timestamp', 'ts', 'event_timestamp'])
        
        for key, value in data.items():
            if key not in used_keys:
                param_record = self.create_param_record(key, value)
                event_params.append(param_record)
        
        return {
            'event_name': event_name,
            'event_timestamp': event_timestamp,
            'event_params': event_params
        }
    
    def parse_url_params(self, message_data):
        """Parse URL parameter string into BigQuery format."""
        try:
            # Parse URL parameters
            params = urllib.parse.parse_qs(message_data)
            
            # Convert values from lists to strings (parse_qs returns lists)
            parsed_params = {}
            for key, value_list in params.items():
                parsed_params[key] = value_list[0] if value_list else ''
            
            # Get event name from various possible parameter names
            event_name_keys = ['en', 'event', 'event_name', 'action']
            event_name = ''
            for key in event_name_keys:
                if key in parsed_params:
                    event_name = parsed_params[key]
                    break
            
            # Get or create timestamp
            event_timestamp = self.get_event_timestamp_from_dict(parsed_params)
            
            # Create event_params from all other parameters
            event_params = []
            used_keys = set(['en', 'event', 'event_name', 'action', 'ts', 'timestamp', 'event_timestamp'])
            
            for key, value in parsed_params.items():
                if key not in used_keys:
                    param_record = self.create_param_record(key, value)
                    event_params.append(param_record)
            
            return {
                'event_name': event_name,
                'event_timestamp': event_timestamp,
                'event_params': event_params
            }
            
        except Exception as e:
            logging.error(f"Error parsing URL parameters: {e}")
            return self.create_error_record()
    
    def get_event_timestamp_from_dict(self, data):
        """Get or create event timestamp in milliseconds."""
        # Check for existing timestamp
        timestamp_keys = ['event_timestamp', 'ts', 'timestamp']
        for key in timestamp_keys:
            if key in data:
                try:
                    # Convert to int
                    ts = int(data[key])
                    # If it looks like seconds (less than year 2100), convert to milliseconds
                    if ts < 4102444800:  # Jan 1, 2100 in seconds
                        ts *= 1000
                    return ts
                except (ValueError, TypeError):
                    continue
        
        # If no timestamp found, use current time in milliseconds
        return int(datetime.utcnow().timestamp() * 1000)
    
    def create_param_record(self, key, value):
        """Create a parameter record for event_params."""
        param_record = {
            'key': str(key),
            'value': {
                'string_value': None,
                'int_value': None,
                'float_value': None,
                'double_value': None
            }
        }
        
        # Determine data type and set appropriate value
        if self.is_integer(value):
            param_record['value']['int_value'] = int(value)
        elif self.is_float(value):
            param_record['value']['double_value'] = float(value)
        else:
            param_record['value']['string_value'] = str(value)
        
        return param_record
    
    def is_integer(self, value):
        """Check if value can be converted to integer."""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def is_float(self, value):
        """Check if value can be converted to float."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def create_error_record(self):
        """Create a default error record for malformed messages."""
        return {
            'event_name': 'ERROR',
            'event_timestamp': int(datetime.utcnow().timestamp() * 1000),
            'event_params': [{
                'key': 'error_type',
                'value': {
                    'string_value': 'PARSE_ERROR',
                    'int_value': None,
                    'float_value': None,
                    'double_value': None
                }
            }]
        }

def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    
    # Changed from input file to Pub/Sub subscription
    parser.add_argument(
        '--input_subscription',
        dest='input_subscription',
        required=False,
        help='Pub/Sub subscription to read from. Format: projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME',
        default='projects/idyllic-ethos-464713-j5/subscriptions/analytics-sub')
    
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='idyllic-ethos-464713-j5.events.web_events')
    
    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # DataIngestion is a class we built in this script to hold the logic for transforming the message into a BigQuery table.
    data_ingestion = DataIngestion()
    
    # Set up pipeline options for streaming
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Initiate the pipeline using the pipeline arguments passed in from the command line.
    p = beam.Pipeline(options=pipeline_options)
    
    (p
     | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
         subscription=known_args.input_subscription,
         with_attributes=True)
     | 'Parse Pub/Sub Message' >> beam.Map(lambda message: data_ingestion.parse_method(message))
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         known_args.output,
         schema='event_name:STRING,event_timestamp:INTEGER,event_params:RECORD',
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
         method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS))
    
    # For streaming pipelines, we don't call wait_until_finish() as it runs indefinitely
    result = p.run()
    
    return result

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
