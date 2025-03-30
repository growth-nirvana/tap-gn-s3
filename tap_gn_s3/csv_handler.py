"""CSV handling utilities."""

import csv
import codecs
from typing import Dict, Iterator

def get_row_iterator(file_handle, table_spec: Dict) -> Iterator[Dict]:
    """Return a row iterator for a CSV file handle.
    
    Args:
        file_handle: File handle for the CSV file
        table_spec: Table specification containing CSV options
        
    Returns:
        Iterator yielding each row as a dictionary
    """
    # Get CSV file parameters from table_spec
    delimiter = table_spec.get("delimiter", ",")
    quotechar = table_spec.get("quotechar", '"')
    encoding = table_spec.get("encoding", "utf-8")
    
    # Create a TextIOWrapper with the specified encoding
    text_stream = codecs.getreader(encoding)(file_handle)
    
    # Create CSV reader
    reader = csv.DictReader(
        text_stream,
        fieldnames=table_spec.get("field_names"),
        delimiter=delimiter,
        quotechar=quotechar,
    )
    
    # Skip header if present
    if not table_spec.get("field_names"):
        next(reader)  # Skip header row if no field names provided
        
    return reader 