#!/usr/bin/env python3

"""
Robust VOEvent XML Parser for GRB Daemon
Handles real-world GCN VOEvent messages properly
"""

import xml.etree.ElementTree as ET
import logging
import math
import time
import re
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime

@dataclass
class GrbTarget:
    """Represents a GRB target with all relevant information."""
    target_id: int = 0
    grb_id: str = ""
    sequence_num: int = 0
    grb_type: int = 0
    ra: float = float('nan')
    dec: float = float('nan')
    error_box: float = float('nan')  # degrees
    detection_time: float = float('nan')
    is_grb: bool = True
    mission: str = "UNKNOWN"
    trigger_num: int = 0
    fluence: float = float('nan')
    peak_flux: float = float('nan')

class VoEventParser:
    """
    Robust parser for VOEvent XML messages from GCN.

    Handles namespaces, complex structures, and mission-specific formats.
    """

    # VOEvent namespace mappings
    NAMESPACES = {
        'voe': 'http://www.ivoa.net/xml/VOEvent/v2.0',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }

    # Common parameter name mappings across missions
    PARAM_MAPPINGS = {
        # Trigger/Event ID variants
        'trigger_id': ['TrigID', 'GraceID', 'EventID', 'trigger_id', 'event_id'],
        'sequence_num': ['Pkt_Ser_Num', 'Sequence_Num', 'sequence_num', 'seq_num'],
        'packet_type': ['Packet_Type', 'PacketType', 'packet_type'],

        # Coordinates - multiple naming conventions
        'ra': ['RA', 'ra', 'Right_Ascension', 'RightAscension'],
        'dec': ['Dec', 'DEC', 'dec', 'Declination', 'declination'],

        # Error measurements
        'error_radius': ['Error2Radius', 'ErrorRadius', 'error_radius', 'positional_error',
                        'Error_Radius', 'Uncertainty_Radius', 'uncertainty'],
        'error_plus': ['Error_Plus', 'error_plus', 'Err_Plus'],
        'error_minus': ['Error_Minus', 'error_minus', 'Err_Minus'],

        # Time variants
        'trigger_time': ['Trigger_TJD', 'TriggerTime', 'trigger_time', 'GRB_Time'],
        'trigger_sod': ['Trigger_SOD', 'SOD', 'sod'],

        # Mission-specific but commonly used
        'importance': ['Importance', 'importance', 'significance'],
        'false_alarm': ['FAR', 'FalseAlarmRate', 'false_alarm_rate'],
    }

    def __init__(self):
        """Initialize the parser."""
        self.mission_parsers = {
            'FERMI': self._parse_fermi_specific,
            'SWIFT': self._parse_swift_specific,
            'MAXI': self._parse_maxi_specific,
            'ICECUBE': self._parse_icecube_specific,
            'SVOM': self._parse_svom_specific,
            'LVC': self._parse_lvc_specific,  # LIGO-Virgo-KAGRA
        }

    def parse_voevent(self, xml_content: str, grb_target: 'GrbTarget') -> 'GrbTarget':
        """
        Parse VOEvent XML content and populate GrbTarget.

        Args:
            xml_content: Raw XML content
            grb_target: GrbTarget object to populate

        Returns:
            Updated GrbTarget object

        Raises:
            Exception: If parsing fails completely
        """
        try:
            # Handle XML declaration and encoding issues
            xml_content = self._clean_xml_content(xml_content)

            # Parse XML with namespace support
            root = ET.fromstring(xml_content)

            # Register namespaces for XPath queries
            for prefix, uri in self.NAMESPACES.items():
                ET.register_namespace(prefix, uri)

            # Extract basic VOEvent metadata
            self._extract_voevent_metadata(root, grb_target)

            # Extract core parameters from What section
            self._extract_what_parameters(root, grb_target)

            # Extract coordinates from WhereWhen section
            self._extract_coordinates(root, grb_target)

            # Extract time information
            self._extract_time_info(root, grb_target)

            # Apply mission-specific parsing
            if grb_target.mission in self.mission_parsers:
                self.mission_parsers[grb_target.mission](root, grb_target)

            # Validate and clean up extracted data
            self._validate_and_cleanup(grb_target)

            logging.debug(f"Successfully parsed VOEvent for {grb_target.mission} trigger {grb_target.grb_id}")
            return grb_target

        except ET.ParseError as e:
            logging.error(f"XML parsing error: {e}")
            # Try fallback regex parsing
            return self._fallback_regex_parse(xml_content, grb_target)

        except Exception as e:
            logging.error(f"VOEvent parsing error: {e}")
            logging.exception("Detailed parsing error:")
            # Return partially parsed target rather than None
            return grb_target

    def _clean_xml_content(self, xml_content: str) -> str:
        """Clean and prepare XML content for parsing."""
        # Handle common encoding issues
        if isinstance(xml_content, bytes):
            xml_content = xml_content.decode('utf-8', errors='replace')

        # Strip BOM if present
        if xml_content.startswith('\ufeff'):
            xml_content = xml_content[1:]

        # Handle malformed XML declarations
        xml_content = re.sub(r'<\?xml[^>]*\?>\s*', '<?xml version="1.0" encoding="UTF-8"?>\n', xml_content)

        return xml_content.strip()

    def _extract_voevent_metadata(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Extract metadata from VOEvent root element."""
        try:
            # Extract IVORN (Identifier)
            ivorn = root.get('ivorn', '')
            if ivorn and not grb_target.grb_id:
                # Try to extract ID from IVORN
                id_match = re.search(r'[_#](\d+)(?:[_.]|$)', ivorn)
                if id_match:
                    grb_target.grb_id = id_match.group(1)

            # Extract role and version
            role = root.get('role', '')
            version = root.get('version', '')

            logging.debug(f"VOEvent metadata: IVORN={ivorn}, role={role}, version={version}")

        except Exception as e:
            logging.warning(f"Error extracting VOEvent metadata: {e}")

    def _extract_what_parameters(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Extract parameters from What section."""
        try:
            # Find What section (handle namespaces)
            what_elements = (
                root.findall('.//What') +
                root.findall('.//{http://www.ivoa.net/xml/VOEvent/v2.0}What') +
                root.findall('.//voe:What', self.NAMESPACES)
            )

            if not what_elements:
                logging.debug("No What section found in VOEvent")
                return

            what = what_elements[0]

            # Extract all parameters
            params = {}
            for param in what.findall('.//Param'):
                name = param.get('name', '')
                value = param.get('value', '')
                unit = param.get('unit', '')
                ucd = param.get('ucd', '')

                if name and value:
                    params[name] = {
                        'value': value,
                        'unit': unit,
                        'ucd': ucd
                    }

            # Map parameters to GrbTarget fields using flexible matching
            self._map_parameters_to_target(params, grb_target)

            logging.debug(f"Extracted {len(params)} parameters from What section")

        except Exception as e:
            logging.warning(f"Error extracting What parameters: {e}")

    def _map_parameters_to_target(self, params: Dict[str, Dict], grb_target: 'GrbTarget') -> None:
        """Map extracted parameters to GrbTarget fields using flexible matching."""

        # Helper function to find parameter by multiple possible names
        def find_param(possible_names: List[str]) -> Optional[Dict]:
            for name in possible_names:
                if name in params:
                    return params[name]
            return None

        # Extract trigger ID
        trigger_param = find_param(self.PARAM_MAPPINGS['trigger_id'])
        if trigger_param and not grb_target.grb_id:
            grb_target.grb_id = str(trigger_param['value'])
            try:
                grb_target.trigger_num = int(trigger_param['value'])
            except (ValueError, TypeError):
                pass

        # Extract sequence number
        seq_param = find_param(self.PARAM_MAPPINGS['sequence_num'])
        if seq_param:
            try:
                grb_target.sequence_num = int(seq_param['value'])
            except (ValueError, TypeError):
                pass

        # Extract packet type
        packet_param = find_param(self.PARAM_MAPPINGS['packet_type'])
        if packet_param:
            try:
                grb_target.grb_type = int(packet_param['value'])
            except (ValueError, TypeError):
                pass

        # Extract coordinates (if not already found in WhereWhen)
        if math.isnan(grb_target.ra):
            ra_param = find_param(self.PARAM_MAPPINGS['ra'])
            if ra_param:
                try:
                    grb_target.ra = float(ra_param['value'])
                except (ValueError, TypeError):
                    pass

        if math.isnan(grb_target.dec):
            dec_param = find_param(self.PARAM_MAPPINGS['dec'])
            if dec_param:
                try:
                    grb_target.dec = float(dec_param['value'])
                except (ValueError, TypeError):
                    pass

        # Extract error radius
        if math.isnan(grb_target.error_box):
            error_param = find_param(self.PARAM_MAPPINGS['error_radius'])
            if error_param:
                try:
                    error_val = float(error_param['value'])
                    unit = error_param.get('unit', '').lower()

                    # Convert to degrees if needed
                    if unit in ['arcmin', 'amin', "'"] or (unit == '' and error_val > 10):
                        error_val = error_val / 60.0
                    elif unit in ['arcsec', 'asec', '"']:
                        error_val = error_val / 3600.0

                    grb_target.error_box = error_val
                except (ValueError, TypeError):
                    pass

    def _extract_coordinates(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Extract coordinates from WhereWhen section."""
        try:
            # Find WhereWhen section
            wherewhen_elements = (
                root.findall('.//WhereWhen') +
                root.findall('.//{http://www.ivoa.net/xml/VOEvent/v2.0}WhereWhen') +
                root.findall('.//voe:WhereWhen', self.NAMESPACES)
            )

            if not wherewhen_elements:
                logging.debug("No WhereWhen section found")
                return

            wherewhen = wherewhen_elements[0]

            # Look for Position2D elements
            position_elements = wherewhen.findall('.//Position2D')

            for pos in position_elements:
                # Check if this is celestial coordinates
                unit = pos.get('unit', 'deg')

                # Find coordinate names
                name1_elem = pos.find('.//Name1')
                name2_elem = pos.find('.//Name2')

                if name1_elem is not None and name2_elem is not None:
                    name1 = name1_elem.text.strip().lower()
                    name2 = name2_elem.text.strip().lower()

                    # Check if this is RA/Dec
                    if 'ra' in name1 and 'dec' in name2:
                        # Extract coordinate values
                        value2_elem = pos.find('.//Value2')
                        if value2_elem is not None:
                            c1_elem = value2_elem.find('.//C1')
                            c2_elem = value2_elem.find('.//C2')

                            if c1_elem is not None and c2_elem is not None:
                                try:
                                    ra = float(c1_elem.text.strip())
                                    dec = float(c2_elem.text.strip())

                                    # Validate ranges
                                    if 0 <= ra <= 360 and -90 <= dec <= 90:
                                        grb_target.ra = ra
                                        grb_target.dec = dec

                                        # Extract error radius if present
                                        error_elem = pos.find('.//Error2Radius')
                                        if error_elem is not None:
                                            try:
                                                error_val = float(error_elem.text.strip())
                                                if unit.lower() in ['arcmin', 'amin']:
                                                    error_val = error_val / 60.0
                                                grb_target.error_box = error_val
                                            except (ValueError, TypeError):
                                                pass

                                        logging.debug(f"Extracted coordinates: RA={ra}, Dec={dec}")
                                        return

                                except (ValueError, TypeError):
                                    continue

        except Exception as e:
            logging.warning(f"Error extracting coordinates: {e}")

    def _extract_time_info(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Extract time information from various sections."""
        try:
            # Look for time in WhereWhen/ObsDataLocation
            time_elements = root.findall('.//TimeInstant//ISOTime')

            for time_elem in time_elements:
                if time_elem.text:
                    try:
                        # Parse ISO format time
                        time_str = time_elem.text.strip()
                        # Handle various ISO formats
                        if 'T' in time_str:
                            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                            grb_target.detection_time = dt.timestamp()
                            logging.debug(f"Extracted time: {time_str} -> {grb_target.detection_time}")
                            return
                    except (ValueError, TypeError):
                        continue

            # Fallback: look for TJD/SOD format in parameters
            self._extract_tjd_time(root, grb_target)

        except Exception as e:
            logging.warning(f"Error extracting time info: {e}")

    def _extract_tjd_time(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Extract time from TJD/SOD format parameters."""
        try:
            tjd = None
            sod = None

            for param in root.findall('.//Param'):
                name = param.get('name', '').lower()
                value = param.get('value', '')

                if 'tjd' in name and 'trigger' in name:
                    try:
                        tjd = int(value)
                    except (ValueError, TypeError):
                        pass
                elif 'sod' in name and 'trigger' in name:
                    try:
                        sod = float(value)
                    except (ValueError, TypeError):
                        pass

            if tjd is not None and sod is not None:
                # Convert TJD + SOD to Unix timestamp
                jd = tjd + 2440000.5  # Convert TJD to JD
                unix_time = (jd - 2440587.5) * 86400 + sod
                grb_target.detection_time = unix_time
                logging.debug(f"Extracted TJD time: TJD={tjd}, SOD={sod} -> {unix_time}")

        except Exception as e:
            logging.warning(f"Error extracting TJD time: {e}")

    def _parse_fermi_specific(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Parse Fermi-specific parameters."""
        try:
            # Fermi-specific parameter extraction
            for param in root.findall('.//Param'):
                name = param.get('name', '')
                value = param.get('value', '')

                if name == 'Reliability':
                    try:
                        # Fermi reliability parameter
                        reliability = float(value)
                        # Could store this in grb_target if needed
                    except (ValueError, TypeError):
                        pass
                elif name == 'Trigger_ID':
                    if not grb_target.grb_id:
                        grb_target.grb_id = str(value)

        except Exception as e:
            logging.warning(f"Error in Fermi-specific parsing: {e}")

    def _parse_swift_specific(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Parse Swift-specific parameters."""
        try:
            # Swift-specific parameter extraction
            for param in root.findall('.//Param'):
                name = param.get('name', '')
                value = param.get('value', '')

                if name == 'TRIGGER_ID':
                    if not grb_target.grb_id:
                        grb_target.grb_id = str(value)
                elif name == 'RATE_SIGNIF':
                    try:
                        # Swift significance
                        significance = float(value)
                        # Could store this in grb_target if needed
                    except (ValueError, TypeError):
                        pass

        except Exception as e:
            logging.warning(f"Error in Swift-specific parsing: {e}")

    def _parse_maxi_specific(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Parse MAXI-specific parameters."""
        # MAXI-specific parsing logic would go here
        pass

    def _parse_icecube_specific(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Parse IceCube-specific parameters."""
        try:
            # IceCube events use different naming conventions
            for param in root.findall('.//Param'):
                name = param.get('name', '')
                value = param.get('value', '')

                if name in ['Event_ID', 'eventid']:
                    grb_target.grb_id = str(value)
                elif name == 'signalness':
                    try:
                        # IceCube signalness parameter
                        signalness = float(value)
                        # Could use this to determine grb_target.is_grb
                    except (ValueError, TypeError):
                        pass

        except Exception as e:
            logging.warning(f"Error in IceCube-specific parsing: {e}")

    def _parse_svom_specific(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Parse SVOM-specific parameters."""
        # SVOM-specific parsing logic would go here
        pass

    def _parse_lvc_specific(self, root: ET.Element, grb_target: 'GrbTarget') -> None:
        """Parse LIGO-Virgo-KAGRA gravitational wave parameters."""
        try:
            # LVC events have different structure
            for param in root.findall('.//Param'):
                name = param.get('name', '')
                value = param.get('value', '')

                if name == 'GraceID':
                    grb_target.grb_id = str(value)
                    grb_target.is_grb = False  # It's a GW event, not GRB
                elif name == 'FAR':
                    try:
                        # False alarm rate
                        far = float(value)
                        grb_target.false_alarm_rate = far
                    except (ValueError, TypeError):
                        pass

        except Exception as e:
            logging.warning(f"Error in LVC-specific parsing: {e}")

    def _validate_and_cleanup(self, grb_target: 'GrbTarget') -> None:
        """Validate and clean up extracted data."""
        # Ensure we have a GRB ID
        if not grb_target.grb_id:
            grb_target.grb_id = f"{grb_target.mission}_UNKNOWN_{int(time.time())}"

        # Validate coordinates
        if not math.isnan(grb_target.ra):
            if not (0 <= grb_target.ra <= 360):
                logging.warning(f"Invalid RA {grb_target.ra}, setting to NaN")
                grb_target.ra = float('nan')

        if not math.isnan(grb_target.dec):
            if not (-90 <= grb_target.dec <= 90):
                logging.warning(f"Invalid Dec {grb_target.dec}, setting to NaN")
                grb_target.dec = float('nan')

        if not math.isnan(grb_target.error_box):
            if grb_target.error_box <= 0:
                logging.warning(f"Invalid error radius {grb_target.error_box}° (must be positive), setting to NaN")
                grb_target.error_box = float('nan')
            elif grb_target.error_box > 180:
                logging.warning(f"Suspiciously large error radius {grb_target.error_box}° (>180°), may be in wrong units")

        # Set detection time if not found
        if math.isnan(grb_target.detection_time):
            grb_target.detection_time = time.time()
            logging.debug("Set detection time to current time")

    def _fallback_regex_parse(self, xml_content: str, grb_target: 'GrbTarget') -> 'GrbTarget':
        """Fallback regex parsing when XML parsing fails."""
        logging.warning("XML parsing failed, using regex fallback")

        try:
            # Your existing regex parsing code would go here
            # This is a simplified version

            # Extract TrigID
            trig_match = re.search(r'TrigID["\s]*value=["\'"]([^"\']*)', xml_content, re.IGNORECASE)
            if trig_match:
                grb_target.grb_id = trig_match.group(1)

            # Extract coordinates
            ra_match = re.search(r'<C1>([^<]*)</C1>', xml_content)
            dec_match = re.search(r'<C2>([^<]*)</C2>', xml_content)

            if ra_match and dec_match:
                try:
                    ra = float(ra_match.group(1))
                    dec = float(dec_match.group(1))
                    if 0 <= ra <= 360 and -90 <= dec <= 90:
                        grb_target.ra = ra
                        grb_target.dec = dec
                except (ValueError, TypeError):
                    pass

            return grb_target

        except Exception as e:
            logging.error(f"Fallback parsing also failed: {e}")
            return grb_target


# Usage in your main grbd.py code:

def _parse_grb_notice_with_robust_xml(self, topic: str, message: str) -> Optional[GrbTarget]:
    """
    REPLACEMENT for _parse_grb_notice() with robust XML parsing.

    This should replace the existing method in grbd.py
    """
    try:
        # Create new target with defaults
        grb = GrbTarget()
        grb.target_id = self.next_target_id
        self.next_target_id += 1

        # Determine mission and type from topic
        if 'FERMI' in topic.upper():
            grb.mission = 'FERMI'
            grb.grb_type = 112
        elif 'SWIFT' in topic.upper():
            grb.mission = 'SWIFT'
            grb.grb_type = 61
        elif 'MAXI' in topic.upper():
            grb.mission = 'MAXI'
            grb.grb_type = 186
        elif 'ICECUBE' in topic.upper():
            grb.mission = 'ICECUBE'
            grb.grb_type = 173
            grb.is_grb = True  # Treat neutrinos as GRBs for follow-up
        elif 'SVOM' in topic.upper():
            grb.mission = 'SVOM'
            grb.grb_type = 200
        elif 'LVC' in topic.upper() or 'LIGO' in topic.upper():
            grb.mission = 'LVC'
            grb.grb_type = 150  # Gravitational wave
            grb.is_grb = False  # Not a GRB, but treat as transient
        else:
            grb.mission = f'UNKNOWN({topic})'
            grb.grb_type = 0

        # Try XML parsing first (most GCN messages are XML)
        if any(marker in message for marker in ['<voe:VOEvent', '<?xml', '<VOEvent']):
            try:
                parser = VoEventParser()
                grb = parser.parse_voevent(message, grb)
                logging.debug(f"Successfully parsed XML for {grb.mission} trigger {grb.grb_id}")
                return grb
            except Exception as e:
                logging.warning(f"XML parsing failed for {topic}, trying text fallback: {e}")

        # Fallback to text/regex parsing for non-XML or malformed XML
        grb = self._parse_text_format_enhanced(message, grb)

        # Final validation
        if not grb.grb_id:
            if grb.trigger_num > 0:
                grb.grb_id = str(grb.trigger_num)
            else:
                grb.grb_id = f"{grb.mission}_EVENT_{int(time.time())}"

        # Set detection time if not found
        if math.isnan(grb.detection_time):
            grb.detection_time = time.time()

        return grb

    except Exception as e:
        logging.error(f"Complete parsing failure for {topic}: {e}")
        logging.exception("Detailed parsing error:")
        return None


def _parse_text_format_enhanced(self, message: str, grb: GrbTarget) -> GrbTarget:
    """
    Enhanced text format parser with better regex patterns.

    This replaces the existing _parse_text_format method.
    """
    try:
        # Enhanced trigger/sequence extraction
        trigger_patterns = [
            r'TRIGGER_NUM:\s*(\d+)',
            r'TrigID["\s]*[:=]\s*["\']?(\d+)',
            r'trigger[_\s]*id["\s]*[:=]\s*["\']?([^"\s,]+)',
            r'Event_ID["\s]*[:=]\s*["\']?([^"\s,]+)',
        ]

        for pattern in trigger_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                grb.grb_id = match.group(1)
                try:
                    grb.trigger_num = int(grb.grb_id)
                except (ValueError, TypeError):
                    pass
                break

        # Enhanced sequence number extraction
        sequence_patterns = [
            r'SEQUENCE_NUM:\s*(\d+)',
            r'Pkt_Ser_Num["\s]*[:=]\s*["\']?(\d+)',
            r'sequence[_\s]*num["\s]*[:=]\s*["\']?(\d+)',
        ]

        for pattern in sequence_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                try:
                    grb.sequence_num = int(match.group(1))
                    break
                except (ValueError, TypeError):
                    pass

        # Enhanced coordinate extraction with multiple formats
        coordinate_patterns = [
            # Standard RA/DEC format
            (r'RA:\s*([\d.]+)', r'DEC?:\s*([-+]?[\d.]+)'),
            # Parameter format
            (r'RA["\s]*[:=]\s*["\']?([\d.]+)', r'Dec["\s]*[:=]\s*["\']?([-+]?[\d.]+)'),
            # XML-like format in text
            (r'<C1>([\d.]+)</C1>', r'<C2>([-+]?[\d.]+)</C2>'),
            # Position format
            (r'Right_Ascension["\s]*[:=]\s*["\']?([\d.]+)', r'Declination["\s]*[:=]\s*["\']?([-+]?[\d.]+)'),
        ]

        for ra_pattern, dec_pattern in coordinate_patterns:
            ra_match = re.search(ra_pattern, message, re.IGNORECASE)
            dec_match = re.search(dec_pattern, message, re.IGNORECASE)

            if ra_match and dec_match:
                try:
                    ra = float(ra_match.group(1))
                    dec = float(dec_match.group(1))

                    # Validate ranges
                    if 0 <= ra <= 360 and -90 <= dec <= 90:
                        grb.ra = ra
                        grb.dec = dec
                        logging.debug(f"Extracted coordinates: RA={ra}, Dec={dec}")
                        break
                except (ValueError, TypeError):
                    continue

        # Enhanced error extraction
        error_patterns = [
            r'ERROR:\s*([\d.]+)',
            r'Error2?Radius["\s]*[:=]\s*["\']?([\d.]+)',
            r'positional[_\s]*error["\s]*[:=]\s*["\']?([\d.]+)',
            r'uncertainty["\s]*[:=]\s*["\']?([\d.]+)',
        ]

        for pattern in error_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                try:
                    error_val = float(match.group(1))
                    # Convert from arcminutes to degrees if likely
                    if error_val > 10:
                        error_val = error_val / 60.0
                    grb.error_box = error_val
                    break
                except (ValueError, TypeError):
                    pass

        # Enhanced time extraction
        time_patterns = [
            r'GRB_TIME:\s*([\d.]+)',
            r'trigger[_\s]*time["\s]*[:=]\s*["\']?([\d.]+)',
            r'detection[_\s]*time["\s]*[:=]\s*["\']?([\d.]+)',
        ]

        for pattern in time_patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                try:
                    grb.detection_time = float(match.group(1))
                    break
                except (ValueError, TypeError):
                    pass

        # Check for test/retraction indicators
        test_indicators = ['TEST', 'RETRACTION', 'PRELIMINARY', 'SIMULATION']
        if any(indicator in message.upper() for indicator in test_indicators):
            grb.is_grb = False
            logging.debug("Detected test/retraction/preliminary event")

        return grb

    except Exception as e:
        logging.error(f"Error in enhanced text parsing: {e}")
        return grb
