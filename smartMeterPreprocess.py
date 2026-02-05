import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


class ConvertUnitsDoFn(beam.DoFn):
    def process(self, element):
        try:
            # Make sure we are actually working with a dictionary
            if not isinstance(element, dict):
                return

            # Copy the record so we are not changing the original object
            converted = element.copy()
            
            # Temperature conversion from Celsius to Fahrenheit
            if converted.get('temperature') is not None:
                celsius = float(converted['temperature'])
                converted['temperature'] = round(celsius * 1.8 + 32, 2)
            
            # Pressure conversion from kPa to psi
            if converted.get('pressure') is not None:
                kpa = float(converted['pressure'])
                converted['pressure'] = round(kpa / 6.895, 4)
            
            yield converted

        except Exception as e:
            # If something goes wrong, log it and keep the pipeline running
            logging.error(f"Error processing record: {e}")


def safe_deserialize(x):
    # Messages from Pub/Sub can come in as bytes or strings
    if isinstance(x, bytes):
        x = x.decode('utf-8')

    # Convert the JSON string into a Python dictionary
    if isinstance(x, str):
        return json.loads(x)

    return x


def filter_complete(element):
    # Skip anything that is not a valid dictionary
    if not isinstance(element, dict):
        return False

    # Only pass through records that have all required sensor values
    return (
        element.get('temperature') is not None and
        element.get('humidity') is not None and
        element.get('pressure') is not None
    )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input Pub/Sub topic')
    parser.add_argument('--output', required=True, help='Output Pub/Sub topic')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Set up pipeline options and enable streaming mode
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Build the Dataflow pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=known_args.input)
            | 'Deserialize message' >> beam.Map(safe_deserialize)
            | 'Filter incomplete records' >> beam.Filter(filter_complete)
            | 'Convert measurement units' >> beam.ParDo(ConvertUnitsDoFn())
            | 'Serialize to bytes' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(topic=known_args.output)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
