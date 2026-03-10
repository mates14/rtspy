#!/usr/bin/env python3
"""
Utilities and examples for the RTS2 Python image processing daemon.

This module provides:
- Command-line tools for interacting with the image processing daemon
- Batch processing utilities
- Image queue management
- Statistics reporting
- Configuration examples
"""

import os
import sys
import time
import argparse
import logging
import glob
from pathlib import Path
from typing import List, Optional, Tuple
import asyncio

from astropy.io import fits
from astropy.time import Time

# Import the image processing daemon
from imgprocd import ImageProcDaemon, ProcessingJob, AstrometryStatus


class ImageProcClient:
    """Client for interacting with running image processing daemon."""
    
    def __init__(self, device_name="IMGP", host="localhost", port=0):
        self.device_name = device_name
        self.host = host
        self.port = port
        # In full implementation, this would connect to the daemon
        # via RTS2 network protocol
    
    def queue_image(self, image_path: str) -> bool:
        """Queue an image for processing."""
        # This would send a command to the daemon
        print(f"Would queue image: {image_path}")
        return True
    
    def get_queue_status(self) -> dict:
        """Get current queue status."""
        # This would query daemon values
        return {
            'queue_size': 0,
            'running_jobs': 0,
            'total_processed': 100,
            'success_rate': 85.5
        }
    
    def get_statistics(self) -> dict:
        """Get processing statistics."""
        return {
            'good_images': 150,
            'trash_images': 20,
            'bad_images': 5,
            'dark_images': 30,
            'flat_images': 25,
            'night_good': 45,
            'night_trash': 8,
            'night_bad': 2
        }
    
    def clear_queue(self) -> bool:
        """Clear the processing queue."""
        print("Would clear processing queue")
        return True
    
    def reset_statistics(self) -> bool:
        """Reset night statistics."""
        print("Would reset night statistics")
        return True


class BatchProcessor:
    """Batch processing utilities for image directories."""
    
    def __init__(self, client: ImageProcClient):
        self.client = client
        
    def process_directory(self, directory: str, pattern: str = "*.fits", 
                         recursive: bool = False) -> List[str]:
        """Process all images in a directory."""
        if recursive:
            pattern = f"**/{pattern}"
            
        image_paths = list(Path(directory).glob(pattern))
        
        print(f"Found {len(image_paths)} images to process")
        
        queued = []
        for image_path in image_paths:
            if self.client.queue_image(str(image_path)):
                queued.append(str(image_path))
                print(f"Queued: {image_path.name}")
            else:
                print(f"Failed to queue: {image_path.name}")
                
        return queued
    
    def reprocess_failed(self, log_file: str) -> List[str]:
        """Reprocess images that previously failed."""
        # This would parse log files to find failed images
        print(f"Would reprocess failed images from log: {log_file}")
        return []


class ImageValidator:
    """Validate FITS images before processing."""
    
    @staticmethod
    def validate_fits(image_path: str) -> Tuple[bool, str]:
        """Validate a FITS file for processing readiness."""
        try:
            with fits.open(image_path) as hdul:
                header = hdul[0].header
                
                # Check required keywords
                required_keywords = ['DATE-OBS', 'EXPTIME']
                missing = []
                for keyword in required_keywords:
                    if keyword not in header:
                        missing.append(keyword)
                
                if missing:
                    return False, f"Missing keywords: {', '.join(missing)}"
                
                # Check exposure time
                exptime = header.get('EXPTIME', 0)
                if exptime <= 0:
                    return False, "Invalid exposure time"
                
                # Check image dimensions
                if len(hdul[0].data.shape) != 2:
                    return False, "Not a 2D image"
                
                return True, "OK"
                
        except Exception as e:
            return False, f"Error reading FITS: {e}"
    
    @staticmethod
    def get_image_info(image_path: str) -> dict:
        """Get basic information about a FITS image."""
        try:
            with fits.open(image_path) as hdul:
                header = hdul[0].header
                data = hdul[0].data
                
                return {
                    'filename': Path(image_path).name,
                    'size_mb': Path(image_path).stat().st_size / 1024 / 1024,
                    'dimensions': data.shape,
                    'object': header.get('OBJECT', 'Unknown'),
                    'filter': header.get('FILTER', 'Unknown'),
                    'exptime': header.get('EXPTIME', 0),
                    'date_obs': header.get('DATE-OBS', ''),
                    'imagetyp': header.get('IMAGETYP', ''),
                    'mean_value': float(data.mean()) if data is not None else 0,
                    'std_value': float(data.std()) if data is not None else 0
                }
        except Exception as e:
            return {'filename': Path(image_path).name, 'error': str(e)}


class StatusReporter:
    """Generate status reports for image processing."""
    
    def __init__(self, client: ImageProcClient):
        self.client = client
        
    def print_status(self):
        """Print current processing status."""
        queue_status = self.client.get_queue_status()
        stats = self.client.get_statistics()
        
        print("\n=== RTS2 Image Processing Status ===")
        print(f"Queue size:      {queue_status['queue_size']}")
        print(f"Running jobs:    {queue_status['running_jobs']}")
        print(f"Total processed: {queue_status['total_processed']}")
        print(f"Success rate:    {queue_status['success_rate']:.1f}%")
        
        print("\n=== Processing Statistics ===")
        print(f"Good astrometry: {stats['good_images']}")
        print(f"No astrometry:   {stats['trash_images']}")
        print(f"Failed:          {stats['bad_images']}")
        print(f"Darks:           {stats['dark_images']}")
        print(f"Flats:           {stats['flat_images']}")
        
        print("\n=== Tonight's Statistics ===")
        print(f"Good astrometry: {stats['night_good']}")
        print(f"No astrometry:   {stats['night_trash']}")
        print(f"Failed:          {stats['night_bad']}")
        
    def generate_report(self, output_file: str):
        """Generate detailed processing report."""
        stats = self.client.get_statistics()
        
        with open(output_file, 'w') as f:
            f.write("RTS2 Image Processing Report\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Generated: {Time.now().iso}\n\n")
            
            f.write("Processing Statistics:\n")
            f.write(f"  Good astrometry: {stats['good_images']}\n")
            f.write(f"  No astrometry:   {stats['trash_images']}\n")
            f.write(f"  Failed:          {stats['bad_images']}\n")
            f.write(f"  Darks:           {stats['dark_images']}\n")
            f.write(f"  Flats:           {stats['flat_images']}\n\n")
            
            total = sum([stats['good_images'], stats['trash_images'], stats['bad_images']])
            if total > 0:
                success_rate = stats['good_images'] / total * 100
                f.write(f"Success rate: {success_rate:.1f}%\n")
        
        print(f"Report saved to: {output_file}")


def validate_directory(directory: str, pattern: str = "*.fits"):
    """Validate all FITS files in a directory."""
    print(f"Validating FITS files in: {directory}")
    
    image_paths = list(Path(directory).glob(pattern))
    print(f"Found {len(image_paths)} files")
    
    valid_count = 0
    invalid_count = 0
    
    for image_path in image_paths:
        is_valid, message = ImageValidator.validate_fits(str(image_path))
        
        if is_valid:
            valid_count += 1
            print(f"✓ {image_path.name}")
        else:
            invalid_count += 1
            print(f"✗ {image_path.name}: {message}")
    
    print(f"\nValidation complete: {valid_count} valid, {invalid_count} invalid")


def inspect_images(directory: str, pattern: str = "*.fits", limit: int = 10):
    """Inspect FITS images and show basic information."""
    print(f"Inspecting FITS files in: {directory}")
    
    image_paths = list(Path(directory).glob(pattern))[:limit]
    
    print(f"\n{'Filename':<20} {'Object':<15} {'Filter':<8} {'ExpTime':<8} {'Size(MB)':<8}")
    print("-" * 70)
    
    for image_path in image_paths:
        info = ImageValidator.get_image_info(str(image_path))
        
        if 'error' in info:
            print(f"{info['filename']:<20} ERROR: {info['error']}")
        else:
            print(f"{info['filename']:<20} {info['object']:<15} {info['filter']:<8} "
                  f"{info['exptime']:<8.1f} {info['size_mb']:<8.1f}")


def setup_example_config():
    """Create an example configuration file."""
    config_content = """[imgproc]
# Image processing configuration

# Astrometry command - can include additional parameters
astrometry_command = solve-field --no-plots --scale-units degwidth --scale-low 0.1 --scale-high 2.0

# Processing timeouts
astrometry_timeout = 3600

# Concurrent processing
max_processes = 2

# Directory paths
archive_path = /data/archive
trash_path = /data/trash
dark_path = /data/darks
flat_path = /data/flats
bad_path = /data/bad

# Standby mode glob pattern
image_glob = /data/incoming/*.fits

[device]
# Device configuration
name = IMGP
port = 0

[logging]
# Logging configuration
level = INFO
file = /var/log/rts2/imgproc.log
"""
    
    config_file = "imgproc_example.conf"
    with open(config_file, 'w') as f:
        f.write(config_content)
    
    print(f"Example configuration saved to: {config_file}")
    print("Edit this file and use with: python imgprocd.py --config imgproc_example.conf")


def main():
    """Main command-line interface."""
    parser = argparse.ArgumentParser(description='RTS2 Image Processing Utilities')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show processing status')
    status_parser.add_argument('--device', default='IMGP', help='Device name')
    
    # Queue command
    queue_parser = subparsers.add_parser('queue', help='Queue images for processing')
    queue_parser.add_argument('path', help='Image file or directory')
    queue_parser.add_argument('--pattern', default='*.fits', help='File pattern for directories')
    queue_parser.add_argument('--recursive', action='store_true', help='Process subdirectories')
    queue_parser.add_argument('--device', default='IMGP', help='Device name')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate FITS files')
    validate_parser.add_argument('directory', help='Directory to validate')
    validate_parser.add_argument('--pattern', default='*.fits', help='File pattern')
    
    # Inspect command
    inspect_parser = subparsers.add_parser('inspect', help='Inspect FITS files')
    inspect_parser.add_argument('directory', help='Directory to inspect')
    inspect_parser.add_argument('--pattern', default='*.fits', help='File pattern')
    inspect_parser.add_argument('--limit', type=int, default=10, help='Maximum files to show')
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Generate processing report')
    report_parser.add_argument('--output', default='processing_report.txt', help='Output file')
    report_parser.add_argument('--device', default='IMGP', help='Device name')
    
    # Config command
    config_parser = subparsers.add_parser('config', help='Generate example configuration')
    
    # Clear command
    clear_parser = subparsers.add_parser('clear', help='Clear processing queue')
    clear_parser.add_argument('--device', default='IMGP', help='Device name')
    
    # Reset command
    reset_parser = subparsers.add_parser('reset', help='Reset statistics')
    reset_parser.add_argument('--device', default='IMGP', help='Device name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    try:
        if args.command == 'status':
            client = ImageProcClient(args.device)
            reporter = StatusReporter(client)
            reporter.print_status()
            
        elif args.command == 'queue':
            client = ImageProcClient(args.device)
            processor = BatchProcessor(client)
            
            if os.path.isfile(args.path):
                # Single file
                if client.queue_image(args.path):
                    print(f"Queued: {args.path}")
                else:
                    print(f"Failed to queue: {args.path}")
            elif os.path.isdir(args.path):
                # Directory
                processor.process_directory(args.path, args.pattern, args.recursive)
            else:
                print(f"Error: Path not found: {args.path}")
                
        elif args.command == 'validate':
            validate_directory(args.directory, args.pattern)
            
        elif args.command == 'inspect':
            inspect_images(args.directory, args.pattern, args.limit)
            
        elif args.command == 'report':
            client = ImageProcClient(args.device)
            reporter = StatusReporter(client)
            reporter.generate_report(args.output)
            
        elif args.command == 'config':
            setup_example_config()
            
        elif args.command == 'clear':
            client = ImageProcClient(args.device)
            if client.clear_queue():
                print("Processing queue cleared")
            else:
                print("Failed to clear queue")
                
        elif args.command == 'reset':
            client = ImageProcClient(args.device)
            if client.reset_statistics():
                print("Statistics reset")
            else:
                print("Failed to reset statistics")
                
    except KeyboardInterrupt:
        print("\nOperation cancelled")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
