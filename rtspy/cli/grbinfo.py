#!/usr/bin/env python3
"""
RTS2 GRB Information Tool - Display detailed information about GRB targets.

This tool queries the RTS2 database for GRB target information and displays
it in a tabulated format including observation details, timing analysis,
and image statistics.

Usage: rts2-grbinfo <target_id>
"""

import sys
import argparse
import psycopg2
from datetime import datetime, timedelta
import math
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

def connect_to_database():
    """Connect to the RTS2 PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="stars",
            user="mates",
            password="pasewcic25"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        return None

def get_grb_info(cursor, target_id=None):
    """Get GRB target information from the database."""
    try:
        if target_id is None:
            # Query for all GRB targets that have some activity (observations or images)
            cursor.execute("""
                SELECT DISTINCT
                    t.tar_id,
                    t.tar_name,
                    t.tar_ra,
                    t.tar_dec,
                    g.grb_id,
                    g.grb_date,
                    g.grb_errorbox,
                    g.grb_is_grb,
                    t.tar_comment,
                    (SELECT MIN(grb_update) FROM grb_gcn WHERE grb_id = g.grb_id) as gcn_reception_time
                FROM targets t
                JOIN grb g ON t.tar_id = g.tar_id
                WHERE EXISTS (
                    SELECT 1 FROM observations o WHERE o.tar_id = t.tar_id
                    UNION
                    SELECT 1 FROM images i 
                    JOIN observations o2 ON i.obs_id = o2.obs_id 
                    WHERE o2.tar_id = t.tar_id
                )
                ORDER BY t.tar_id
            """)
            
            results = cursor.fetchall()
            return [
                {
                    'tar_id': row[0],
                    'tar_name': row[1],
                    'tar_ra': float(row[2]) if row[2] is not None else None,
                    'tar_dec': float(row[3]) if row[3] is not None else None,
                    'grb_id': row[4],
                    'grb_date': row[5],
                    'grb_errorbox': float(row[6]) if row[6] is not None else None,
                    'grb_is_grb': row[7],
                    'tar_comment': row[8],
                    'gcn_reception_time': row[9]
                }
                for row in results
            ]
        else:
            # Query for specific GRB target including GCN reception time
            cursor.execute("""
                SELECT 
                    t.tar_id,
                    t.tar_name,
                    t.tar_ra,
                    t.tar_dec,
                    g.grb_id,
                    g.grb_date,
                    g.grb_errorbox,
                    g.grb_is_grb,
                    t.tar_comment,
                    (SELECT MIN(grb_update) FROM grb_gcn WHERE grb_id = g.grb_id) as gcn_reception_time
                FROM targets t
                JOIN grb g ON t.tar_id = g.tar_id
                WHERE t.tar_id = %s
            """, (target_id,))
            
            grb_info = cursor.fetchone()
            if not grb_info:
                return None
                
            return {
                'tar_id': grb_info[0],
                'tar_name': grb_info[1],
                'tar_ra': float(grb_info[2]) if grb_info[2] is not None else None,
                'tar_dec': float(grb_info[3]) if grb_info[3] is not None else None,
                'grb_id': grb_info[4],
                'grb_date': grb_info[5],
                'grb_errorbox': float(grb_info[6]) if grb_info[6] is not None else None,
                'grb_is_grb': grb_info[7],
                'tar_comment': grb_info[8],
                'gcn_reception_time': grb_info[9]
            }
        
    except Exception as e:
        print(f"Error querying GRB information: {e}", file=sys.stderr)
        return None

def get_observation_info(cursor, target_id):
    """Get observation information for the target."""
    try:
        cursor.execute("""
            SELECT 
                obs_id,
                obs_start,
                obs_end,
                obs_state
            FROM observations 
            WHERE tar_id = %s 
            ORDER BY obs_start
        """, (target_id,))
        
        observations = cursor.fetchall()
        return [
            {
                'obs_id': obs[0],
                'obs_start': obs[1],
                'obs_end': obs[2],
                'obs_state': obs[3]
            }
            for obs in observations
        ]
        
    except Exception as e:
        print(f"Error querying observation information: {e}", file=sys.stderr)
        return []

def get_image_info(cursor, target_id):
    """Get image information for the target."""
    try:
        # Get all images for observations of this target
        cursor.execute("""
            SELECT 
                i.obs_id,
                i.img_id,
                i.img_date,
                i.img_usec,
                i.img_exposure,
                i.camera_name,
                i.mount_name,
                i.filter_id
            FROM images i
            JOIN observations o ON i.obs_id = o.obs_id
            WHERE o.tar_id = %s
            ORDER BY i.img_date, i.img_usec
        """, (target_id,))
        
        images = cursor.fetchall()
        return [
            {
                'obs_id': img[0],
                'img_id': img[1],
                'img_date': img[2],
                'img_usec': img[3] or 0,
                'img_exposure': img[4],
                'camera_name': img[5],
                'mount_name': img[6],
                'filter_id': img[7]
            }
            for img in images
        ]
        
    except Exception as e:
        print(f"Error querying image information: {e}", file=sys.stderr)
        return []

def format_coordinates(ra, dec):
    """Format RA/Dec coordinates for display."""
    if ra is None or dec is None:
        return "No coordinates"
    
    # Convert RA to hours:minutes:seconds
    ra_hours = ra / 15.0
    ra_h = int(ra_hours)
    ra_m = int((ra_hours - ra_h) * 60)
    ra_s = ((ra_hours - ra_h) * 60 - ra_m) * 60
    
    # Convert Dec to degrees:arcminutes:arcseconds
    dec_sign = "+" if dec >= 0 else "-"
    dec_abs = abs(dec)
    dec_d = int(dec_abs)
    dec_m = int((dec_abs - dec_d) * 60)
    dec_s = ((dec_abs - dec_d) * 60 - dec_m) * 60
    
    return f"{ra_h:02d}:{ra_m:02d}:{ra_s:06.3f} {dec_sign}{dec_d:02d}:{dec_m:02d}:{dec_s:05.2f}"

def format_time_difference(time_diff_seconds):
    """
    Format time difference for display with dynamic range from milliseconds to years.
    Returns a compact, space-free string suitable for pipe-friendly output.
    
    Args:
        time_diff_seconds: Time difference in seconds (can be float)
    
    Returns:
        Formatted string without spaces
    """
    if time_diff_seconds is None:
        return "-"
    
    abs_seconds = abs(time_diff_seconds)
    
    # Milliseconds (< 1 second)
    if abs_seconds < 1.0:
        ms = abs_seconds * 1000
        return f"{ms:.1f}ms"
    
    # Seconds (< 1 minute)
    elif abs_seconds < 60:
        return f"{abs_seconds:.3f}s"
    
    # Minutes and seconds (< 1 hour)
    elif abs_seconds < 3600:
        minutes = int(abs_seconds // 60)
        seconds = abs_seconds % 60
        return f"{minutes}m{seconds:.1f}s"
    
    # Hours (< 1.5 days)
    elif abs_seconds < 1.5 * 86400:
        hours = abs_seconds / 3600
        return f"{hours:.3f}h"
    
    # Days (< 1 month ~30 days)
    elif abs_seconds < 30 * 86400:
        days = abs_seconds / 86400
        if days < 10:
            return f"{days:.1f}d"
        else:
            return f"{days:.0f}d"
    
    # Months (< 1 year)
    elif abs_seconds < 365.25 * 86400:
        months = abs_seconds / (30.44 * 86400)  # Average month length
        return f"{months:.1f}mo"
    
    # Years
    else:
        years = abs_seconds / (365.25 * 86400)
        if years < 10:
            return f"{years:.1f}y"
        else:
            return f"{years:.0f}y"

def calculate_time_differences(grb_date, first_image_time, last_image_time=None, gcn_reception_time=None):
    """Calculate time differences for display."""
    if not grb_date or not first_image_time:
        return "-", "-", "-"

    # Normalize all datetimes to naive (remove timezone info if present)
    if grb_date.tzinfo is not None:
        grb_date = grb_date.replace(tzinfo=None)
    if first_image_time.tzinfo is not None:
        first_image_time = first_image_time.replace(tzinfo=None)

    # Time difference between first image and trigger
    trigger_to_first = first_image_time - grb_date
    trigger_to_first_str = format_time_difference(trigger_to_first.total_seconds())

    # GCN reception time (if available)
    if gcn_reception_time:
        if gcn_reception_time.tzinfo is not None:
            gcn_reception_time = gcn_reception_time.replace(tzinfo=None)
        gcn_to_first = first_image_time - gcn_reception_time
        gcn_to_first_str = format_time_difference(gcn_to_first.total_seconds())
    else:
        gcn_to_first_str = "-"

    # Calculate time spent on target (first to last image time)
    if last_image_time and last_image_time != first_image_time:
        if last_image_time.tzinfo is not None:
            last_image_time = last_image_time.replace(tzinfo=None)
        time_on_target = last_image_time - first_image_time
        time_on_target_str = format_time_difference(time_on_target.total_seconds())
    else:
        time_on_target_str = "-"

    return trigger_to_first_str, gcn_to_first_str, time_on_target_str

def extract_telescope_info(observations, images):
    """Extract telescope/site information from observations and images."""
    if not observations and not images:
        return "-", "-"
    
    # Try to get mount/camera info from images
    if images:
        mount_name = images[0].get('mount_name', '-')
        camera_name = images[0].get('camera_name', '-')
        
        # Map mount names to sites (customize as needed)
        if 'mates' in mount_name.lower():
            site = "La Palma"
        elif 'auger' in mount_name.lower():
            site = "Auger"
        else:
            site = mount_name or "-"
            
        return site, camera_name
    
    return "-", "-"

def extract_grb_name(tar_name, tar_comment, grb_id):
    """Extract GRB name from target information."""
    if not tar_name:
        return f"GRB {grb_id}"
    
    # Look for GRB names in the target name or comment
    if "GRB" in tar_name:
        return tar_name
    elif tar_comment and "GRB" in tar_comment:
        # Try to extract GRB name from comment
        import re
        grb_match = re.search(r'GRB\s*\d{6}[A-Z]?', tar_comment, re.IGNORECASE)
        if grb_match:
            return grb_match.group()
    
    return tar_name or f"Target {grb_id}"

def load_grb_config(config_file='grbs.yaml'):
    """Load GRB supplementary information from YAML config file."""
    if not YAML_AVAILABLE:
        return {'grbs': {}, 'defaults': {}, 'site_name': 'FRAM'}
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        # Return defaults if config file doesn't exist
        return {
            'grbs': {},
            'defaults': {
                'status': 'trigger follow-up',
                'obs_status': ''
            },
            'site_name': 'FRAM'
        }
    except Exception as e:
        print(f"Warning: Error loading config file {config_file}: {e}", file=sys.stderr)
        return {'grbs': {}, 'defaults': {}, 'site_name': 'FRAM'}

def get_grb_supplementary_info(grb_info, config):
    """Get supplementary information for a GRB from config."""
    tar_id = grb_info['tar_id']
    grb_entry = config.get('grbs', {}).get(tar_id, {})
    defaults = config.get('defaults', {})
    
    # Extract GCN ID from grb_id or tar_comment for link generation
    gcn_id = grb_info.get('grb_id', '')
    
    result = {
        'gcn_link': grb_entry.get('gcn_link', ''),
        'grb_name': grb_entry.get('grb_name', ''),
        'grb_link': grb_entry.get('grb_link', ''),
        'status': grb_entry.get('status', defaults.get('status', 'trigger follow-up')),
        'obs_status': grb_entry.get('obs_status', defaults.get('obs_status', 'pending'))
    }
    
    return result

def should_include_grb(grb_info, first_image_time, max_delay_seconds):
    """Check if GRB should be included based on response delay."""
    if max_delay_seconds is None:
        return True

    if not grb_info['grb_date'] or not first_image_time:
        return True  # Include if we can't calculate delay

    # Normalize both datetimes to naive (remove timezone info if present)
    grb_date = grb_info['grb_date']
    if grb_date.tzinfo is not None:
        grb_date = grb_date.replace(tzinfo=None)
    if first_image_time.tzinfo is not None:
        first_image_time = first_image_time.replace(tzinfo=None)

    # Calculate delay in seconds
    delay = first_image_time - grb_date
    delay_seconds = delay.total_seconds()

    return delay_seconds <= max_delay_seconds

def generate_html_table(all_grb_data, config):
    """Generate HTML table matching the FRAM format."""
    site_name = config.get('site_name', 'FRAM')
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{site_name} Trigger and Burst REAL-TIME Information</title>
    <style type="text/css">
    .smallfont, .smallfont TD, .smallfont TH {{
        font-family: times;
        font-size: 10pt;
    }}
    </style>
</head>
<body>

<h1>{site_name} Trigger and Burst Information</h1>
<hr>

<table border="2" class="smallfont">
<caption><b>{site_name} GRB TRIGGERED OBSERVATION</b></caption>

<tr align="left">
<th colspan="3">GRB/TRIGGER</th>
<th colspan="20">OBSERVATION</th>
</tr>

<tr align="left">
<th>Trig</th>
<th>==Date===</th>
<th>Time UT</th>
<th>RA</th><th>Dec</th><th>Error</th>
<th>Tel.</th>
<th>ID</th>
<th>GRB</th>
<th>Tobs-T0</th>
<th>Tobs-Ttrig</th>
<th>Tspent</th>
<th>====GRB=status=====</th>
<th>good</th>
<th>bad</th>
<th>Obs. status</th>
</tr>

"""

    # Group by year for separator rows
    current_year = None
    
    for data in sorted(all_grb_data, key=lambda x: x['grb_info']['grb_date'] or datetime.min, reverse=True):
        grb_info = data['grb_info']
        observations = data['observations'] 
        images = data['images']
        timing = data['timing']
        supplementary = data['supplementary']
        
        # Add year separator
        if grb_info['grb_date']:
            year = grb_info['grb_date'].year
            if current_year != year:
                current_year = year
                html += f'<tr bgcolor="lightyellow"><td colspan="16" align="center">{year}</td></tr>\\n'
        
        # Extract data for table row
        tar_id = grb_info['tar_id']
        grb_date = grb_info['grb_date']
        date_str = grb_date.strftime('%Y-%m-%d') if grb_date else "-"
        time_str = grb_date.strftime('%H:%M:%S.%f')[:-3] if grb_date else "-"
        
        # Coordinates
        ra_str = f"{grb_info['tar_ra']:.6f}" if grb_info['tar_ra'] is not None else "-"
        dec_str = f"{grb_info['tar_dec']:+.6f}" if grb_info['tar_dec'] is not None else "-"
        
        # Error box
        errorbox = grb_info['grb_errorbox']
        if errorbox is None:
            error_str = "-"
        elif errorbox > 1:
            error_str = f"{errorbox:.1f}"
        elif errorbox * 60 > 1:
            error_str = f"{errorbox*60:.1f}"
        else:
            error_str = f"{errorbox*3600:.1f}"
            
        # Site (simplified)
        site = config.get('site_name', 'FRAM')
        
        # GRB name and link
        grb_name = supplementary['grb_name']
        if grb_name and supplementary['grb_link']:
            grb_cell = f'<a href="{supplementary["grb_link"]}">{grb_name}</a>'
        elif grb_name:
            grb_cell = grb_name
        else:
            grb_cell = ""
            
        # GCN link  
        if supplementary['gcn_link']:
            gcn_cell = f'<a href="{supplementary["gcn_link"]}">{grb_info["grb_id"]}</a>'
        else:
            gcn_cell = grb_info['grb_id'] or "-"
            
        # Timing
        tobs_t0 = timing['trigger_to_first']
        tobs_ttrig = timing['gcn_to_first'] 
        tspent = timing['time_on_target']
        
        # Image counts
        num_images = len(images)
        good_images = len([img for img in images if img.get('process_bitfield', 0) > 0])
        bad_images = num_images - good_images
        
        html += f"""<tr>
<td>{gcn_cell}</td>
<td>{date_str}</td>
<td>{time_str}</td>
<td>{ra_str}</td>
<td>{dec_str}</td>
<td>{error_str}</td>
<td>{site}</td>
<td>{tar_id}</td>
<td>{grb_cell}</td>
<td>{tobs_t0}</td>
<td>{tobs_ttrig}</td>
<td>{tspent}</td>
<td>{supplementary['status']}</td>
<td>{good_images}</td>
<td>{bad_images}</td>
<td>{supplementary['obs_status']}</td>
</tr>
"""

    html += """
</table>

</body>
</html>"""
    
    return html

def display_grb_info(cursor, grb_info, output_format='text'):
    """Display information for a single GRB target."""
    # Get observation and image information
    observations = get_observation_info(cursor, grb_info['tar_id'])
    images = get_image_info(cursor, grb_info['tar_id'])
    
    # Extract information for display
    target_id = grb_info['tar_id']
    grb_date = grb_info['grb_date']
    grb_date_str = grb_date.strftime('%Y-%m-%d') if grb_date else "-"
    grb_time_str = grb_date.strftime('%H:%M:%S.%f')[:-3] if grb_date else "-"
    
    coords_str = format_coordinates(grb_info['tar_ra'], grb_info['tar_dec'])
    
    errorbox = grb_info['grb_errorbox']
    if errorbox is None:
        errorbox_str = "-"
    elif errorbox > 1:
        errorbox_str = f"{errorbox:5.1f}deg"
    elif 60*errorbox > 1:
        errorbox_str = f"{60*errorbox:5.1f}min"
    else:
        errorbox_str = f"{3600*errorbox:5.1f}sec"
    
    site, camera = extract_telescope_info(observations, images)
    
    grb_name = extract_grb_name(grb_info['tar_name'], grb_info['tar_comment'], grb_info['grb_id'])
    
    # Calculate timing information
    first_image_time = None
    last_image_time = None
    if images:
        first_img = images[0]
        if first_img['img_date']:
            first_image_time = first_img['img_date']
            if first_img['img_usec']:
                first_image_time += timedelta(microseconds=first_img['img_usec'])
        
        # Find last image time
        last_img = images[-1]
        if last_img['img_date']:
            last_image_time = last_img['img_date']
            if last_img['img_usec']:
                last_image_time += timedelta(microseconds=last_img['img_usec'])
    
    trigger_to_first, gcn_to_first, time_on_target = calculate_time_differences(
        grb_date, first_image_time, last_image_time, grb_info.get('gcn_reception_time')
    )
    
    # Count images
    num_images = len(images)
    processed_images = len([img for img in images if img.get('process_bitfield', 0) > 0])
    
    # Determine classification
    is_grb = grb_info.get('grb_is_grb', False)
    if is_grb:
        classification = "(real)"
    else:
        classification = "(fake)"
    
    # Format output line
    print(f"{target_id:<5} {grb_date_str:<10} {grb_time_str:<11} {coords_str:<26} "
          f"{errorbox_str} {site:<2} "
          f"{trigger_to_first:<8} {gcn_to_first:<8} {time_on_target:<8} "
          f"{num_images:<3} {processed_images:<3} {classification:<6} {grb_name:<30}")

def main():
    parser = argparse.ArgumentParser(
        description='Display detailed information about RTS2 GRB targets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  rts2-grbinfo 1100848    # Show info for target ID 1100848
  rts2-grbinfo --all      # Show all GRB targets with activity

The output format matches the RTS2 GRB log format:
target_id    date         trigger_time    coordinates              errorbox    site         gcn_id       name         timing1      timing2      delay    comment      images   processed
        """
    )
    
    parser.add_argument('target_id', type=int, nargs='?',
                      help='Target ID to query information for')
    parser.add_argument('--all', action='store_true',
                      help='Show all GRB targets with observations or images')
    parser.add_argument('--html', action='store_true',
                      help='Generate HTML output instead of text')
    parser.add_argument('--config', default='grbs.yaml',
                      help='YAML config file for supplementary GRB information')
    parser.add_argument('--max-delay', type=int, default=None,
                      help='Maximum trigger-to-first-image delay in seconds to include (default: no limit for text, 3600s for HTML)')
    
    args = parser.parse_args()
    
    if not args.target_id and not args.all:
        parser.error("Please specify a target ID or use --all")
    
    # Load configuration
    config = load_grb_config(args.config)
    
    # Connect to database
    conn = connect_to_database()
    if not conn:
        return 1
    
    try:
        cursor = conn.cursor()
        
        if args.all:
            # Get all GRB targets with activity
            all_grb_info = get_grb_info(cursor)
            if not all_grb_info:
                print("No GRB targets with activity found", file=sys.stderr)
                return 1
            
            if args.html:
                # Set default max delay for HTML mode if not specified
                max_delay = args.max_delay if args.max_delay is not None else 3600
                
                # Collect all data for HTML generation
                all_grb_data = []
                for grb_info in all_grb_info:
                    observations = get_observation_info(cursor, grb_info['tar_id'])
                    images = get_image_info(cursor, grb_info['tar_id'])
                    
                    # Calculate timing
                    first_image_time = None
                    last_image_time = None
                    if images:
                        first_img = images[0]
                        if first_img['img_date']:
                            first_image_time = first_img['img_date']
                            if first_img['img_usec']:
                                first_image_time += timedelta(microseconds=first_img['img_usec'])
                        
                        last_img = images[-1]
                        if last_img['img_date']:
                            last_image_time = last_img['img_date']
                            if last_img['img_usec']:
                                last_image_time += timedelta(microseconds=last_img['img_usec'])
                    
                    # Apply delay filter for HTML output
                    if not should_include_grb(grb_info, first_image_time, max_delay):
                        continue
                    
                    trigger_to_first, gcn_to_first, time_on_target = calculate_time_differences(
                        grb_info['grb_date'], first_image_time, last_image_time, grb_info.get('gcn_reception_time')
                    )
                    
                    timing = {
                        'trigger_to_first': trigger_to_first,
                        'gcn_to_first': gcn_to_first,
                        'time_on_target': time_on_target
                    }
                    
                    supplementary = get_grb_supplementary_info(grb_info, config)
                    
                    all_grb_data.append({
                        'grb_info': grb_info,
                        'observations': observations,
                        'images': images,
                        'timing': timing,
                        'supplementary': supplementary
                    })
                
                # Generate and print HTML
                html_output = generate_html_table(all_grb_data, config)
                print(html_output)
            else:
                # Text output - process each target
                for grb_info in all_grb_info:
                    # Apply delay filter if specified
                    if args.max_delay is not None:
                        images = get_image_info(cursor, grb_info['tar_id'])
                        first_image_time = None
                        if images:
                            first_img = images[0]
                            if first_img['img_date']:
                                first_image_time = first_img['img_date']
                                if first_img['img_usec']:
                                    first_image_time += timedelta(microseconds=first_img['img_usec'])
                        
                        if not should_include_grb(grb_info, first_image_time, args.max_delay):
                            continue
                    
                    display_grb_info(cursor, grb_info)
            
            return 0
    
        # Single target mode
        grb_info = get_grb_info(cursor, args.target_id)
        if not grb_info:
            print(f"No GRB target found with ID {args.target_id}", file=sys.stderr)
            return 1
        
        # Text output only for single target (HTML doesn't make much sense for one target)
        display_grb_info(cursor, grb_info)
        
        # If verbose mode was requested, show additional details
        if len(sys.argv) > 2 and '--verbose' in sys.argv:
            print("\nDetailed Information:")
            print(f"  Target ID: {target_id}")
            print(f"  Target Name: {grb_info['tar_name']}")
            print(f"  GRB ID: {grb_info['grb_id']}")
            print(f"  Coordinates: RA={grb_info['tar_ra']:.6f}°, Dec={grb_info['tar_dec']:.6f}°")
            print(f"  Error Box: {errorbox_str}°")
            print(f"  Trigger Time: {grb_date}")
            print(f"  Is GRB: {is_grb}")
            print(f"  Observations: {len(observations)}")
            print(f"  Images: {num_images}")
            if grb_info['tar_comment']:
                print(f"  Comment: {grb_info['tar_comment']}")
                
    except Exception as e:
        print(f"Error querying database: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
