#!/usr/bin/env python3

"""
GRB Daemon - Python implementation for RTS2 using modern GCN Kafka interface.

This daemon receives GRB alerts from NASA's General Coordinates Network (GCN)
via Kafka and integrates them into the RTS2 observatory control system.

Based on the original RTS2 grbd.cpp but modernized to use:
- Python RTS2 framework
- Modern GCN Kafka interface instead of socket-based protocol
- Improved error handling and configuration
"""

import logging
import time
import threading
import json
import re
import os
import math
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timezone
from dataclasses import dataclass, field
import psycopg2

try:
    from gcn_kafka import Consumer
    GCN_KAFKA_AVAILABLE = True
except ImportError:
    logging.warning("gcn-kafka not available. Install with: pip install gcn-kafka")
    GCN_KAFKA_AVAILABLE = False

from constants import DeviceType
from device import Device
from config import SimpleDeviceConfig
from value import (ValueBool, ValueString, ValueInteger, ValueTime,
                  ValueDouble, ValueRaDec, ValueAltAz)
from app import App


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


class GcnKafkaConsumer:
    """Handles GCN Kafka message consumption and parsing."""

    def __init__(self, client_id: str, client_secret: str, domain: str = "gcn.nasa.gov"):
        """
        Initialize GCN Kafka consumer.

        Args:
            client_id: GCN client ID
            client_secret: GCN client secret
            domain: GCN domain (production, test, or dev)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.domain = domain
        self.consumer = None
        self.running = False
        self.thread = None
        self.message_callback = None

        # Topics to subscribe to for GRB alerts and pointing directions
        self.topics = [
            # GRB coordinate topics
                    'gcn.classic.voevent.FERMI_GBM_ALERT',
                    'gcn.classic.voevent.FERMI_GBM_FIN_POS',
                    'gcn.classic.voevent.FERMI_GBM_FLT_POS',
                    'gcn.classic.voevent.FERMI_GBM_GND_POS',
                    'gcn.classic.voevent.FERMI_GBM_SUBTHRESH',
                    'gcn.classic.voevent.MAXI_UNKNOWN',
                    'gcn.classic.voevent.SWIFT_BAT_GRB_POS_ACK',
                    'gcn.classic.voevent.SWIFT_BAT_QL_POS',
                    'gcn.classic.text.ICECUBE_ASTROTRACK_GOLD',
                    'gcn.classic.text.ICECUBE_ASTROTRACK_BRONZE',
                    # SVOM mission topics (as in example.py)
                    'gcn.notices.svom.voevent.grm',
                    'gcn.notices.svom.voevent.eclairs',
                    'gcn.notices.svom.voevent.mxt',
                    # 'gcn.classic.text.LVC_INITIAL',
                    # 'gcn.classic.text.LVC_UPDATE',

            # Additional mission topics that might be available
            # Note: Some topics from original grbd.py may not exist
            # We should only subscribe to topics that actually exist
        ]

    def start(self) -> bool:
        """Start the Kafka consumer in a separate thread."""
        if not GCN_KAFKA_AVAILABLE:
            logging.error("Cannot start GCN consumer: gcn-kafka package not available")
            return False

        try:
            # Create consumer with persistent group ID for message recovery
            config = {
                'broker.address.family': 'v4',
#                'group.id': f'rts2-grbd-{self.client_id}',
#                'auto.offset.reset': 'latest',  # Start from latest messages
#                'enable.auto.commit': True,     # Auto-commit offsets
            }

            self.consumer = Consumer(
                config=config,
                client_id=self.client_id,
                client_secret=self.client_secret,
#                domain=self.domain
            )
            logging.info(f"Connected to GCN Kafka")

            # Subscribe to topics
            self.consumer.subscribe(self.topics)
            logging.info(f"Subscribed to {len(self.topics)} GCN topics")

            # Start consumer thread
            self.running = True
            self.thread = threading.Thread(
                target=self._consumer_loop,
                name="GCN-Consumer",
                daemon=True
            )
            self.thread.start()

            return True

        except Exception as e:
            logging.error(f"Failed to start GCN Kafka consumer: {e}")
            return False

    def stop(self):
        """Stop the Kafka consumer."""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
        if self.consumer:
            self.consumer.close()

    def set_message_callback(self, callback: Callable[[str, str], None]):
        """Set callback for processing received messages."""
        self.message_callback = callback

    def _consumer_loop(self):
        """Main consumer loop running in separate thread."""
        logging.info("GCN Kafka consumer loop started")

        while self.running:
            try:
                # Consume messages with timeout
                for message in self.consumer.consume(timeout=1):
                    if message.error():
                        logging.error(f"Kafka error: {message.error()}")
                        continue

                    if not self.running:
                        break

                    # Process message
                    topic = message.topic()
                    value = message.value()

                    if value and self.message_callback:
                        try:
                            # Print like in example.py for debugging
                            logging.debug(f'topic={topic}, offset={message.offset()}')

                            # Decode message value
                            text_value = value.decode('utf-8') if isinstance(value, bytes) else str(value)

                            # Call message handler
                            self.message_callback(topic, text_value)

                        except Exception as e:
                            logging.error(f"Error processing message from {topic}: {e}")

            except Exception as e:
                logging.error(f"Error in GCN consumer loop: {e}")
                if self.running:
                    time.sleep(5.0)  # Back off on error

        logging.info("GCN Kafka consumer thread stopped")


class GrbDaemon(Device, SimpleDeviceConfig):
    """
    GRB Daemon - receives GCN alerts via Kafka and triggers observations.

    This daemon connects to NASA's General Coordinates Network via Kafka
    to receive gamma-ray burst alerts and other transient notifications.
    """

    def setup_config(self, config):
        """
        Register GRB daemon configuration arguments.
        called automatically from the confguration system
        """

        # GCN connection options
        config.add_argument('--gcn-client-id', help='GCN Kafka client ID', default='ibkn5j8ocfhhdftq6feedfmdv')
        config.add_argument('--gcn-client-secret', help='GCN Kafka client secret', default='rh3v8p9sqq9o0q8goqufdl2449eq7fl37hf74096c5km9l6cjm')
        config.add_argument('--gcn-domain', default='gcn.nasa.gov', choices=['gcn.nasa.gov', 'test.gcn.nasa.gov', 'dev.gcn.nasa.gov'], help='GCN Kafka domain')

        # GRB processing options
        config.add_argument('--disable-grbs', action='store_true', help='Disable GRB observations (monitoring only)')
        config.add_argument('--create-disabled', action='store_true', help='Create GRB targets disabled for automatic follow-up')
        config.add_argument('--queue-to', help='Queue name for GRB observations')
        config.add_argument('--add-exec', help='Execute command when new GCN packet arrives')
        config.add_argument('--exec-followups', action='store_true', help='Execute observations for follow-ups without error box')

        # Visibility filtering
        config.add_argument('--not-visible', action='store_true', default=True, help='Record GRBs not visible from current location')
        config.add_argument('--only-visible-tonight', action='store_true', help='Record only GRBs visible during current night')
        config.add_argument('--min-grb-altitude', type=float, default=0.0, help='Minimum GRB altitude to consider as visible (degrees)')

    def __init__(self, device_name="GRBD", port=0):
        """Initialize GRB daemon."""
        super().__init__(device_name, DeviceType.GRB, port)

        # Configuration parameters (will be set by process_args)
        self.gcn_client_id = None
        self.gcn_client_secret = None
        self.gcn_domain = "gcn.nasa.gov"
        self.disable_grbs = False
        self.create_disabled = False
        self.queue_name = None
        self.add_exec = None
        self.exec_followups = False
        self.record_not_visible = True
        self.record_only_visible_tonight = False
        self.min_grb_altitude = 0.0

        # GCN Kafka consumer
        self.gcn_consumer = None

        # Current GRB tracking
        self.current_grb = None
        self.grb_targets = {}  # target_id -> GrbTarget
        self.next_target_id = 1

        # Statistics
        self.packets_received = 0
        self.grbs_processed = 0
        self.last_packet_time = 0.0

        # Create RTS2 values for monitoring
        self._create_values()

        # Set initial state
        self.set_state(self.STATE_IDLE, "GRB daemon initializing")

    def apply_config(self, config: Dict[str, Any]):
        """Apply GRB-specific configuration."""
        super().apply_config(config)

        # Apply GCN configuration
        self.gcn_client_id = config.get('gcn_client_id')
        self.gcn_client_secret = config.get('gcn_client_secret')

        # Apply GRB configuration
        self.gcn_domain = config.get('gcn_domain', 'gcn.nasa.gov')
        self.disable_grbs = config.get('disable_grbs', False)
        self.create_disabled = config.get('create_disabled', False)
        self.queue_name = config.get('queue_to')
        self.add_exec = config.get('add_exec')
        self.exec_followups = config.get('exec_followups', False)
        self.record_not_visible = config.get('not_visible', True)
        self.record_only_visible_tonight = config.get('only_visible_tonight', False)
        self.min_grb_altitude = config.get('min_grb_altitude', 0.0)

    def _create_values(self):
        """Create RTS2 values for monitoring and control."""

        # Control values (automatically registered)
        self.grb_enabled = ValueBool("enabled", "GRB reception enabled", default=not self.disable_grbs, writable=True)
        self.create_disabled_val = ValueBool("create_disabled", "Create GRBs disabled for auto-observation", default=self.create_disabled, writable=True)
        self.record_not_visible_val = ValueBool("not_visible", "Record GRBs not visible from location", default=self.record_not_visible, writable=True)
        self.only_visible_tonight_val = ValueBool("only_visible_tonight", "Record only GRBs visible tonight", default=self.record_only_visible_tonight, writable=True)
        self.min_altitude_val = ValueDouble("min_grb_altitude", "Minimum GRB altitude for visibility", default=self.min_grb_altitude, writable=True)

        # Status values (automatically registered)
        self.last_packet = ValueTime("last_packet", "Time of last GCN packet")
        self.packets_received_val = ValueInteger("packets_received", "Total GCN packets received")
        self.grbs_processed_val = ValueInteger("grbs_processed", "Total GRBs processed")

        # Last GRB information
        self.last_target = ValueString("last_target", "Name of last GRB target")
        self.last_target_id = ValueInteger("last_target_id", "ID of last GRB target")
        self.last_target_time = ValueTime("last_target_time", "Time of last target")
        self.last_target_type = ValueInteger("last_target_type", "Type of last target")
        self.last_target_radec = ValueRaDec("last_target_radec", "Coordinates (J2000) of last GRB")
        self.last_target_errorbox = ValueDouble("last_target_errorbox", "Error box of last target (degrees)")

        # Spacecraft pointing information
        self.last_swift_time = ValueTime("last_swift", "Time of last Swift position")
        self.last_swift_radec = ValueRaDec("last_swift_position", "Swift current position")
        self.last_svom_time = ValueTime("last_svom", "Time of last INTEGRAL position")
        self.last_svom_radec = ValueRaDec("last_svom_position", "INTEGRAL current position")

    def start(self):
        """Start the GRB daemon."""
        super().start()

        # Validate configuration
        if not self.gcn_client_id or not self.gcn_client_secret:
            logging.error("GCN client ID and secret must be provided")
            self.set_state(self.STATE_IDLE | self.ERROR_HW,
                          "Missing GCN credentials")
            return

        logging.info(f"GRB daemon starting with GCN domain: {self.gcn_domain}")
        logging.info(f"GCN Client ID: {self.gcn_client_id}")
        if self.disable_grbs:
            logging.info("GRB observations DISABLED - monitoring only")

        # Initialize GCN Kafka consumer
        self.gcn_consumer = GcnKafkaConsumer(
            client_id=self.gcn_client_id,
            client_secret=self.gcn_client_secret,
            domain=self.gcn_domain
        )
        self.gcn_consumer.set_message_callback(self._on_gcn_message)

        # Start consumer
        if self.gcn_consumer.start():
            logging.info(f"GRB daemon started, connected to GCN at {self.gcn_domain}")
            self.set_ready("Connected to GCN Kafka")
        else:
            logging.error("Failed to start GCN consumer")
            self.set_state(self.STATE_IDLE | self.ERROR_HW,
                          "Failed to connect to GCN")

    def stop(self):
        """Stop the GRB daemon."""
        if self.gcn_consumer:
            self.gcn_consumer.stop()
        super().stop()

    def info(self):
        """Update device information."""
        super().info()

        # Update packet statistics
        self.packets_received_val.value = self.packets_received
        self.grbs_processed_val.value = self.grbs_processed

        if self.last_packet_time > 0:
            self.last_packet.value = self.last_packet_time

        # Update current GRB information if available
        if self.current_grb:
            self.last_target.value = self.current_grb.grb_id
            self.last_target_id.value = self.current_grb.target_id
            self.last_target_time.value = self.current_grb.detection_time
            self.last_target_type.value = self.current_grb.grb_type

            if not (math.isnan(self.current_grb.ra) or math.isnan(self.current_grb.dec)):
                self.last_target_radec.value = (self.current_grb.ra, self.current_grb.dec)

            if not math.isnan(self.current_grb.error_box):
                self.last_target_errorbox.value = self.current_grb.error_box

    def _on_gcn_message(self, topic: str, message: str):
        """
        Handle incoming GCN message from Kafka.

        Args:
            topic: Kafka topic name
            message: Message content as string
        """
        try:
            # Update statistics
            self.packets_received += 1
            self.last_packet_time = time.time()

            logging.debug(f"Processing GCN message from {topic}")

            # Parse message based on topic type
            if 'POINTDIR' in topic:
                self._process_pointing_message(topic, message)
            elif any(grb_type in topic for grb_type in ['GRB', 'GBM', 'BAT', 'INTEGRAL', 'AGILE', 'MAXI']):
                self._process_grb_message(topic, message)
            elif 'LVC' in topic:
                self._process_gravitational_wave_message(topic, message)
            elif 'ICECUBE' in topic:
                self._process_neutrino_message(topic, message)
            else:
                logging.debug(f"Unhandled topic type: {topic}")

        except Exception as e:
            logging.error(f"Error processing GCN message from {topic}: {e}")

    def _process_grb_message(self, topic: str, message: str):
        """Process GRB coordinate messages."""
        try:
            # Parse GRB information from message
            grb_info = self._parse_grb_notice(topic, message)
            if not grb_info:
                return

            logging.info(f"Received GRB alert: {grb_info.grb_id} "
                        f"from {grb_info.mission} "
                        f"RA/Dec={grb_info.ra:.3f},{grb_info.dec:.3f} "
                        f"Error={grb_info.error_box:.3f} deg")

            if not (math.isnan(grb_info.ra) or math.isnan(grb_info.dec)):
                logging.info(f"  Position: RA={grb_info.ra:.3f}, Dec={grb_info.dec:.3f}")
            if not math.isnan(grb_info.error_box):
                logging.info(f"  Error box: {grb_info.error_box:.3f} degrees")

            # Add to database and get target ID
            target_id = self._add_grb_to_database(grb_info)
            if target_id:
                grb_info.target_id = target_id

                # Store GRB information
                self.grb_targets[target_id] = grb_info
                self.current_grb = grb_info
                self.grbs_processed += 1

                # Check visibility if filtering enabled
                if not self._check_visibility_constraints(grb_info):
                    logging.info(f"GRB {grb_info.grb_id} not visible, skipping observation")
                    return

                # Execute external command if configured
                if self.add_exec:
                    self._execute_external_command(grb_info)

                # Trigger GRB observation if enabled
                if self.grb_enabled.value and grb_info.is_grb:
                    self._trigger_grb_observation(target_id)
                else:
                    logging.info(f"GRB observation disabled or not a GRB: {grb_info.grb_id}")

        except Exception as e:
            logging.error(f"Error processing GRB message: {e}")

    def _add_grb_to_database(self, grb: GrbTarget) -> Optional[int]:
        """
        Add GRB target to RTS2 PostgreSQL database with proper deduplication.

        Args:
            grb: GRB target to add

        Returns:
            Target ID if successful, None otherwise
        """
        try:
            # Connect to RTS2 PostgreSQL database
            conn = psycopg2.connect(
                host="localhost",
                database="stars",  # Default RTS2 database name
                user="rts2",
                password=""  # Assumes peer authentication or configured password
            )
            cursor = conn.cursor()

            # Convert string grb_id to integer for database
            try:
                grb_id_int = int(grb.grb_id)
            except ValueError:
                # If grb_id is not numeric, hash it to get an integer
                grb_id_int = abs(hash(grb.grb_id)) % 2147483647  # Keep within int range

            logging.debug(f"Processing GRB ID: {grb.grb_id} -> {grb_id_int}")

            # Step 1: Check for existing GRB by exact identifiers first
            cursor.execute("""
                SELECT tar_id, grb_errorbox, grb_is_grb, grb_ra, grb_dec, grb_date
                FROM grb
                WHERE grb_id = %s AND grb_seqn = %s AND grb_type = %s
            """, (grb_id_int, grb.sequence_num, grb.grb_type))

            existing_exact = cursor.fetchone()

            # Step 2: Check for existing GRBs from different missions/types within time/position window
            time_window = 900  # 15 min in seconds
            existing_related = None

            if not existing_exact and not math.isnan(grb.detection_time) and not math.isnan(grb.ra) and not math.isnan(grb.dec):
                # Look for GRBs within time and position window
                cursor.execute("""
                    SELECT g.tar_id, g.grb_id, g.grb_type, g.grb_ra, g.grb_dec, g.grb_date, g.grb_errorbox,
                           t.tar_name
                    FROM grb g
                    JOIN targets t ON g.tar_id = t.tar_id
                    WHERE ABS(EXTRACT(EPOCH FROM g.grb_date) - %s) < %s
                      AND g.grb_ra IS NOT NULL AND g.grb_dec IS NOT NULL
                      AND g.grb_is_grb = true
                """, (grb.detection_time, time_window))

                candidates = cursor.fetchall()

                for candidate in candidates:
                    (cand_tar_id, cand_grb_id, cand_grb_type, cand_ra, cand_dec,
                     cand_date, cand_errorbox, cand_name) = candidate

                    # Calculate angular separation
                    angular_sep = self._calculate_angular_separation(
                        grb.ra, grb.dec, cand_ra, cand_dec
                    )

                    # Check if positions are consistent within error boxes
                    max_error = max(
                        grb.error_box if not math.isnan(grb.error_box) else 5.0,
                        cand_errorbox if cand_errorbox and not math.isnan(cand_errorbox) else 5.0,
                        0.5  # Minimum 0.5 degree tolerance
                    )

                    if angular_sep <= max_error:
                        logging.info(f"Found related GRB: {cand_name} (ID: {cand_grb_id}, "
                                   f"type: {cand_grb_type}) at {angular_sep:.3f}° separation")
                        existing_related = candidate
                        break

            # Step 3: Process based on what we found
            if existing_exact:
                # Update existing exact match if this has better accuracy
                return self._update_existing_grb(cursor, conn, grb, grb_id_int, existing_exact)

            elif existing_related:
                # Link to existing related GRB if position/time match
                return self._link_to_related_grb(cursor, conn, grb, grb_id_int, existing_related)

            else:
                # Create completely new GRB target
                return self._create_new_grb_target(cursor, conn, grb, grb_id_int)

        except Exception as e:
            logging.error(f"Error adding GRB to database: {e}")
            if 'conn' in locals():
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
            return None


    def _update_existing_grb(self, cursor, conn, grb: GrbTarget, grb_id_int: int, existing):
        """Update existing GRB with potentially better information."""
        existing_tar_id, existing_error, existing_is_grb, existing_ra, existing_dec, existing_date = existing

        # Check if we should update (better error box or missing coordinates)
        should_update = (
            existing_error is None or
            math.isnan(existing_error) or
            (not math.isnan(grb.error_box) and grb.error_box < existing_error) or
            (existing_ra is None or existing_dec is None) or
            (not math.isnan(grb.ra) and not math.isnan(grb.dec))
        )

        if should_update:
            # Update target coordinates if we have them
            if not math.isnan(grb.ra) and not math.isnan(grb.dec):
                cursor.execute("""
                    UPDATE targets
                    SET tar_ra = %s, tar_dec = %s, tar_enabled = %s
                    WHERE tar_id = %s
                """, (grb.ra, grb.dec, not self.create_disabled_val.value, existing_tar_id))

            # Update GRB information
            cursor.execute("""
                UPDATE grb
                SET grb_ra = %s, grb_dec = %s, grb_is_grb = %s,
                    grb_last_update = to_timestamp(%s), grb_errorbox = %s
                WHERE grb_id = %s AND grb_seqn = %s AND grb_type = %s
            """, (grb.ra, grb.dec, grb.is_grb, time.time(), grb.error_box,
                  grb_id_int, grb.sequence_num, grb.grb_type))

            logging.info(f"Updated GRB target: ID={existing_tar_id}, {grb.grb_id}")
            if not math.isnan(existing_error) and not math.isnan(grb.error_box):
                logging.info(f"  Error improved: {existing_error:.3f}° -> {grb.error_box:.3f}°")

        else:
            logging.info(f"GRB {grb.grb_id} update ignored - no improvement")

        # Always add raw packet data for archival
        self._add_gcn_raw_packet(cursor, grb, grb_id_int)

        conn.commit()
        conn.close()
        return existing_tar_id


    def _link_to_related_grb(self, cursor, conn, grb: GrbTarget, grb_id_int: int, related):
        """Link new GRB detection to existing related target."""
        (related_tar_id, related_grb_id, related_grb_type, related_ra, related_dec,
         related_date, related_errorbox, related_name) = related

        logging.info(f"Linking GRB {grb.grb_id} to existing target {related_name} (ID: {related_tar_id})")

        # Insert new GRB record pointing to same target
        cursor.execute("""
            INSERT INTO grb (
                tar_id, grb_id, grb_seqn, grb_type, grb_ra, grb_dec,
                grb_is_grb, grb_date, grb_last_update, grb_errorbox
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s)
        """, (related_tar_id, grb_id_int, grb.sequence_num, grb.grb_type,
              grb.ra, grb.dec, grb.is_grb, grb.detection_time,
              time.time(), grb.error_box))

        # Update target if this detection has better accuracy
        if (not math.isnan(grb.error_box) and
            (related_errorbox is None or math.isnan(related_errorbox) or
             grb.error_box < related_errorbox)):

            cursor.execute("""
                UPDATE targets
                SET tar_ra = %s, tar_dec = %s
                WHERE tar_id = %s
            """, (grb.ra, grb.dec, related_tar_id))

            logging.info(f"  Updated target position with better accuracy: {grb.error_box:.3f}°")

        # Add raw packet data
        self._add_gcn_raw_packet(cursor, grb, grb_id_int)

        conn.commit()
        conn.close()
        return related_tar_id


    def _create_new_grb_target(self, cursor, conn, grb: GrbTarget, grb_id_int: int):
        """Create completely new GRB target."""

        # Generate new target ID using the sequence
        cursor.execute("SELECT nextval('grb_tar_id')")
        tar_id = cursor.fetchone()[0]

        # Generate target name in RTS2 format
        grb_time = datetime.fromtimestamp(grb.detection_time)
        if grb.mission == 'ICECUBE':
            target_name = f"IceCube {grb_time.strftime('%y%m%d.%f')[:-3]} trigger #{grb.grb_id}"
        else:
            target_name = f"GRB {grb_time.strftime('%y%m%d.%f')[:-3]} GCN #{grb.grb_id}"

        # Create target comment
        comment = (f"Generated by GRBD for event {grb_time.isoformat()}, "
                  f"GCN #{grb.grb_id}, type {grb.grb_type}")

        # Determine if target should be enabled
        tar_enabled = not self.create_disabled_val.value

        # Insert into targets table
        cursor.execute("""
            INSERT INTO targets (
                tar_id, type_id, tar_name, tar_ra, tar_dec,
                tar_enabled, tar_comment, tar_priority, tar_bonus, tar_bonus_time
            ) VALUES (%s, 'G', %s, %s, %s, %s, %s, 100, 100, NULL)
        """, (tar_id, target_name, grb.ra, grb.dec, tar_enabled, comment))

        # Insert into grb table
        cursor.execute("""
            INSERT INTO grb (
                tar_id, grb_id, grb_seqn, grb_type, grb_ra, grb_dec,
                grb_is_grb, grb_date, grb_last_update, grb_errorbox
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s)
        """, (tar_id, grb_id_int, grb.sequence_num, grb.grb_type,
              grb.ra, grb.dec, grb.is_grb, grb.detection_time,
              time.time(), grb.error_box))

        logging.info(f"Created new GRB target: ID={tar_id}, {grb.grb_id} at "
                   f"RA={grb.ra:.3f}, Dec={grb.dec:.3f}, Error={grb.error_box:.3f}°")

        # Add raw GCN packet data
        self._add_gcn_raw_packet(cursor, grb, grb_id_int)

        conn.commit()
        conn.close()
        return tar_id


    def _calculate_angular_separation(self, ra1, dec1, ra2, dec2):
        """
        Calculate angular separation between two positions in degrees.

        Uses the haversine formula for spherical coordinates.
        """
        import math

        # Convert to radians
        ra1_rad = math.radians(ra1)
        dec1_rad = math.radians(dec1)
        ra2_rad = math.radians(ra2)
        dec2_rad = math.radians(dec2)

        # Haversine formula
        delta_ra = ra2_rad - ra1_rad
        delta_dec = dec2_rad - dec1_rad

        a = (math.sin(delta_dec/2)**2 +
             math.cos(dec1_rad) * math.cos(dec2_rad) * math.sin(delta_ra/2)**2)

        c = 2 * math.asin(math.sqrt(a))

        # Convert back to degrees
        return math.degrees(c)

    def _add_gcn_raw_packet(self, cursor, grb: GrbTarget, grb_id_int: int):
        """Add raw GCN packet data to grb_gcn table for archival."""
        try:
            # Create a simplified packet array representing the VOEvent data
            # In the original C++ code, this was the actual GCN packet data
            packet_data = [0] * 40  # 40-element array as in original
            packet_data[0] = grb_id_int
            packet_data[1] = grb.sequence_num
            packet_data[2] = grb.grb_type
            packet_data[3] = int(grb.ra * 10000) if not math.isnan(grb.ra) else 0
            packet_data[4] = int(grb.dec * 10000) if not math.isnan(grb.dec) else 0
            packet_data[5] = int(grb.error_box * 10000) if not math.isnan(grb.error_box) else 0

            # Use PostgreSQL array type
            cursor.execute("""
                INSERT INTO grb_gcn (
                    grb_id, grb_seqn, grb_type, grb_update, grb_update_usec, packet
                ) VALUES (%s, %s, %s, to_timestamp(%s), %s, %s)
            """, (grb_id_int, grb.sequence_num, grb.grb_type,
                  int(time.time()), int((time.time() % 1) * 1000000),
                  packet_data))  # PostgreSQL handles array directly

        except Exception as e:
            logging.warning(f"Failed to store raw GCN packet: {e}")

    def _check_visibility_constraints(self, grb: GrbTarget) -> bool:
        """
        Check visibility constraints for GRB.

        Replicates the visibility logic from the original addGcnPoint function.
        """
        # If recording all GRBs regardless of visibility
        if self.record_not_visible_val.value:
            return True

        # Basic declination check (simplified - would need proper observatory coordinates)
        if not math.isnan(grb.dec):
            # Assume observatory at latitude 50° (should come from configuration)
            observer_lat = 50.0

            # Object never visible if dec < (observer_lat - 90)
            if grb.dec < (observer_lat - 90):
                return False

            # Object never visible if dec > (observer_lat + 90) for southern observers
            if observer_lat < 0 and grb.dec > (observer_lat + 90):
                return False

        # Check "only visible tonight" constraint
        if self.only_visible_tonight_val.value:
            # This would require detailed rise/set calculations
            # For now, just do a simplified check
            current_time = time.time()
            # Simplified: check if object is at reasonable hour angle
            # In full implementation, would use libnova equivalent calculations
            pass

        return True
    def _process_gravitational_wave_message(self, topic: str, message: str):
        """Process LIGO-Virgo-KAGRA gravitational wave alerts."""
        # For now, just log - could be extended for electromagnetic counterpart searches
        logging.info(f"Received gravitational wave alert from {topic}")

    def _process_neutrino_message(self, topic: str, message: str):
        """Process IceCube neutrino alerts."""
        # For now, just log - could be extended for multi-messenger followup
        logging.info(f"Received neutrino alert from {topic}")

    def _parse_grb_notice(self, topic: str, message: str) -> Optional[GrbTarget]:
        """
        Parse GRB notice from GCN VOEvent message.

        Returns:
            GrbTarget object or None if parsing failed
        """
        try:
            # Create new GRB target with defaults
            grb = GrbTarget()
            grb.target_id = self.next_target_id
            self.next_target_id += 1

            # Determine mission and type from topic
            grb_type_map = {
                'FERMI_GBM_FLT_POS': 111,    # TYPE_FERMI_GBM_FLT_POS
                'FERMI_GBM_GND_POS': 112,    # TYPE_FERMI_GBM_GND_POS
                'FERMI_GBM_FIN_POS': 115,    # TYPE_FERMI_GBM_FIN_POS
                'FERMI_GBM_ALERT': 110,      # TYPE_FERMI_GBM_ALERT
                'SWIFT_BAT_GRB_POS_ACK': 61, # TYPE_SWIFT_BAT_GRB_POS_ACK_SRC
                'SWIFT_BAT_QL_POS': 61,      # TYPE_SWIFT_BAT_QL_POS
                'MAXI_UNKNOWN': 186,         # TYPE_MAXI_UNKNOWN
                'ICECUBE_ASTROTRACK_GOLD': 173,   # TYPE_ICECUBE_ASTROTRACK_GOLD
                'ICECUBE_ASTROTRACK_BRONZE': 174, # TYPE_ICECUBE_ASTROTRACK_BRONZE
                'SVOM_GRM': 200,             # SVOM GRM alerts
                'SVOM_ECLAIRS': 201,         # SVOM ECLAIRS alerts
                'SVOM_MXT': 202,             # SVOM MXT alerts
            }

            # Extract mission and type from topic
            for key, type_id in grb_type_map.items():
                if key in topic.upper():
                    grb.grb_type = type_id
                    grb.mission = key.split('_')[0]
                    break
            else:
                grb.mission = 'UNKNOWN'
                grb.grb_type = 0

            # VOEvent parsing - look for XML-like structure or key-value pairs
            # Many VOEvents contain both XML structure and text parameters

            # Try to parse as VOEvent XML first
            if '<voe:VOEvent' in message or '<?xml' in message:
                grb = self._parse_voevent_xml(message, grb)
            else:
                # Fallback to text parsing for mixed format messages
                grb = self._parse_text_format(message, grb)

            # Validate that we got essential information
            if not grb.grb_id:
                if grb.trigger_num > 0:
                    grb.grb_id = str(grb.trigger_num)
                else:
                    grb.grb_id = f"{grb.mission}_GRB_{int(time.time())}"

            # Set detection time if not found
            if math.isnan(grb.detection_time):
                grb.detection_time = time.time()

            return grb

        except Exception as e:
            logging.error(f"Error parsing GRB notice: {e}")
            return None

    def _parse_voevent_xml(self, message: str, grb: GrbTarget) -> GrbTarget:
        """Parse VOEvent XML format message."""
        try:
            # Simple regex-based XML parsing for key fields
            # In production, would use proper XML parser, but this works for most cases

            # Extract TrigID/GraceID (GRB identifier)
            trig_match = re.search(r'<Param\s+name="TrigID"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if not trig_match:
                trig_match = re.search(r'<Param\s+name="GraceID"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if trig_match:
                grb.grb_id = trig_match.group(1)
                try:
                    grb.trigger_num = int(grb.grb_id)
                except:
                    pass

            # Extract sequence number
            seq_match = re.search(r'<Param\s+name="Pkt_Ser_Num"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if seq_match:
                try:
                    grb.sequence_num = int(seq_match.group(1))
                except:
                    pass

            # Extract coordinates from C1 and C2 parameters
            ra_match = re.search(r'<Param\s+name="RA"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if not ra_match:
                ra_match = re.search(r'<C1>([^<]*)</C1>', message)
            if ra_match:
                try:
                    grb.ra = float(ra_match.group(1))
                except:
                    pass

            dec_match = re.search(r'<Param\s+name="Dec"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if not dec_match:
                dec_match = re.search(r'<C2>([^<]*)</C2>', message)
            if dec_match:
                try:
                    grb.dec = float(dec_match.group(1))
                except:
                    pass

            # Extract error circle/box
            error_match = re.search(r'<Param\s+name="Error2Radius"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if not error_match:
                error_match = re.search(r'<Param\s+name="ErrorRadius"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if not error_match:
                error_match = re.search(r'<Error2Radius>([^<]*)</Error2Radius>', message)
            if error_match:
                try:
                    error_val = float(error_match.group(1))
                    # Convert from arcminutes to degrees if needed
                    if error_val > 10:  # Assume arcminutes if > 10
                        grb.error_box = error_val / 60.0
                    else:
                        grb.error_box = error_val
                except:
                    pass

            # Extract time - look for GRB_TIME or trigger time
            time_match = re.search(r'<Param\s+name="GRB_Time"[^>]*value="([^"]*)"', message, re.IGNORECASE)
            if not time_match:
                time_match = re.search(r'<Param\s+name="Trigger_TJD"[^>]*value="([^"]*)"', message, re.IGNORECASE)
                if time_match:
                    # Handle TJD format
                    sod_match = re.search(r'<Param\s+name="Trigger_SOD"[^>]*value="([^"]*)"', message, re.IGNORECASE)
                    if sod_match:
                        try:
                            tjd = int(time_match.group(1))
                            sod = float(sod_match.group(1))
                            # Convert TJD + SOD to Unix timestamp
                            jd = tjd + 2440000.5  # Convert TJD to JD
                            grb.detection_time = (jd - 2440587.5) * 86400 + sod
                        except:
                            pass
            else:
                try:
                    grb.detection_time = float(time_match.group(1))
                except:
                    pass

            # Check if it's actually a GRB (vs. test or other event)
            if any(word in message.upper() for word in ['TEST', 'RETRACTION', 'PRELIMINARY']):
                grb.is_grb = False

            return grb

        except Exception as e:
            logging.error(f"Error parsing VOEvent XML: {e}")
            return grb

    def _parse_text_format(self, message: str, grb: GrbTarget) -> GrbTarget:
        """Parse text format message (fallback)."""
        try:
            # Extract trigger/sequence information
            trigger_match = re.search(r'TRIGGER_NUM:\s*(\d+)', message, re.IGNORECASE)
            if trigger_match:
                grb.grb_id = trigger_match.group(1)
                grb.trigger_num = int(trigger_match.group(1))

            sequence_match = re.search(r'SEQUENCE_NUM:\s*(\d+)', message, re.IGNORECASE)
            if sequence_match:
                grb.sequence_num = int(sequence_match.group(1))

            # Extract coordinates
            ra_match = re.search(r'RA:\s*([\d.]+)', message, re.IGNORECASE)
            dec_match = re.search(r'DEC:\s*([-+]?[\d.]+)', message, re.IGNORECASE)

            if ra_match and dec_match:
                grb.ra = float(ra_match.group(1))
                grb.dec = float(dec_match.group(1))

            # Extract error box
            error_match = re.search(r'ERROR:\s*([\d.]+)', message, re.IGNORECASE)
            if error_match:
                error_val = float(error_match.group(1))
                # Convert from arcminutes to degrees if needed
                if error_val > 10:  # Assume arcminutes if > 10
                    grb.error_box = error_val / 60.0
                else:
                    grb.error_box = error_val

            # Extract time
            time_match = re.search(r'GRB_TIME:\s*([\d.]+)', message, re.IGNORECASE)
            if time_match:
                grb.detection_time = float(time_match.group(1))

            return grb

        except Exception as e:
            logging.error(f"Error parsing text format: {e}")
            return grb

    def _execute_external_command(self, grb: GrbTarget):
        """Execute external command for GRB processing."""
        try:
            import subprocess

            # Build command arguments as specified in original grbd
            # Format: target-id grb-id grb-seqn grb-type grb-ra grb-dec grb-is-grb grb-date grb-errorbox
            cmd_args = [
                self.add_exec,
                str(grb.target_id),
                grb.grb_id,
                str(grb.sequence_num),
                str(grb.grb_type),
                f"{grb.ra:.6f}",
                f"{grb.dec:.6f}",
                "1" if grb.is_grb else "0",
                f"{grb.detection_time:.6f}",
                f"{grb.error_box:.6f}"
            ]

            logging.info(f"Executing external command: {' '.join(cmd_args)}")

            # Execute command asynchronously
            subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        except Exception as e:
            logging.error(f"Error executing external command: {e}")

    def _trigger_grb_observation(self, target_id: int):
        """
        Trigger GRB observation through RTS2 executor or queue.

        Args:
            target_id: Target ID to observe
        """
        try:
            logging.info(f"Triggering GRB observation for target {target_id}")

            # First try to find an executor connection
            executor_conn = None
            for conn in self.network.connection_manager.connections.values():
                if (hasattr(conn, 'remote_device_type') and
                    conn.remote_device_type == DeviceType.EXECUTOR and
                    conn.state == ConnectionState.AUTH_OK):
                    executor_conn = conn
                    break

            if executor_conn:
                # Send execute GRB command to executor
                cmd = f"grb {target_id}" # or now <tar_id>
                executor_conn.send_command(cmd, self._on_execute_grb_result)
                logging.info(f"Sent GRB execute command to executor: {cmd}")

            elif self.queue_name:
                # Queue GRB for selector
                self._queue_grb_observation(target_id)

            else:
                logging.error(f"No executor available and no queue specified for GRB {target_id}")

        except Exception as e:
            logging.error(f"Error triggering GRB observation: {e}")

    def _queue_grb_observation(self, target_id: int):
        """Queue GRB observation with selector."""
        try:
            # Find selector connections
            selector_conns = []
            for conn in self.network.connection_manager.connections.values():
                if (hasattr(conn, 'remote_device_type') and
                    conn.remote_device_type == DeviceType.SELECTOR and
                    conn.state == ConnectionState.AUTH_OK):
                    selector_conns.append(conn)

            if not selector_conns:
                logging.error(f"No selector available to queue GRB {target_id}")
                return

            # Send queue command to all selectors
            for conn in selector_conns:
                cmd = f"queue_now_once {self.queue_name} {target_id}"
                conn.send_command(cmd, self._on_queue_grb_result)
                logging.info(f"Queued GRB {target_id} to selector {conn.name}")

        except Exception as e:
            logging.error(f"Error queuing GRB observation: {e}")

    def _on_execute_grb_result(self, conn, success, code, message):
        """Handle result from executor GRB command."""
        if success:
            logging.info(f"GRB execution successful: {message}")
        else:
            logging.error(f"GRB execution failed: {message}")
            # Try queuing if execution failed and queue is configured
            if self.queue_name and self.current_grb:
                self._queue_grb_observation(self.current_grb.target_id)

    def _on_queue_grb_result(self, conn, success, code, message):
        """Handle result from selector queue command."""
        if success:
            logging.info(f"GRB queued successfully: {message}")
        else:
            logging.error(f"GRB queue failed: {message}")

    # RTS2 command handlers
    def on_enabled_changed(self, old_value, new_value):
        """Handle changes to GRB enabled state."""
        if new_value:
            logging.info("GRB processing enabled")
        else:
            logging.info("GRB processing disabled")

    def on_create_disabled_changed(self, old_value, new_value):
        """Handle changes to create disabled state."""
        self.create_disabled = new_value
        logging.info(f"Create disabled set to: {new_value}")

    def on_not_visible_changed(self, old_value, new_value):
        """Handle changes to record not visible state."""
        self.record_not_visible = new_value
        logging.info(f"Record not visible set to: {new_value}")

    def on_only_visible_tonight_changed(self, old_value, new_value):
        """Handle changes to only visible tonight state."""
        self.record_only_visible_tonight = new_value
        logging.info(f"Only visible tonight set to: {new_value}")

    def on_min_grb_altitude_changed(self, old_value, new_value):
        """Handle changes to minimum GRB altitude."""
        self.min_grb_altitude = new_value
        logging.info(f"Minimum GRB altitude set to: {new_value} degrees")


# Additional command handlers for RTS2 integration
class GrbCommands:
    """Command handlers specific to GRB daemon."""

    def __init__(self, grb_daemon):
        self.grb_daemon = grb_daemon
        self.handlers = {
            "test": self.handle_test_grb,
        }
        self.needs_response = {
            "test": True,
        }

    def get_commands(self):
        """Get list of commands this handler supports."""
        return list(self.handlers.keys())

    def can_handle(self, command):
        """Check if this handler can process a command."""
        return command in self.handlers

    def needs_response_for(self, command):
        """Check if command needs a response."""
        return self.needs_response.get(command, True)

    def handle(self, command, conn, params):
        """Dispatch command to appropriate handler."""
        if command in self.handlers:
            return self.handlers[command](conn, params)
        return False

    def handle_test_grb(self, conn, params):
        """Handle test GRB trigger command."""
        try:
            parts = params.split()
            if len(parts) != 1:
                return False

            target_id = int(parts[0])

            # Trigger test GRB observation
            if self.grb_daemon._trigger_grb_observation(target_id):
                return True
            else:
                return False

        except Exception as e:
            logging.error(f"Error in test GRB command: {e}")
            return False


def main():
    """Main entry point for GRB daemon."""

    # Create application
    app = App(description='RTS2 GRB Daemon with GCN Kafka Support')

    # Register device options
    app.register_device_options(GrbDaemon)

    # Parse arguments
    args = app.parse_args()

    # Validate required arguments
    if not args.gcn_client_id or not args.gcn_client_secret:
        logging.error("GCN client ID and secret are required")
        logging.error("Get credentials from: https://gcn.nasa.gov/")
        logging.error("Use --gcn-client-id and --gcn-client-secret options")
        return 1

    if False:
        # Test database connection
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="stars",
                user="rts2",
                password=""
            )
            conn.close()
            logging.info("PostgreSQL database connection verified")
        except Exception as e:
            logging.error(f"Cannot connect to RTS2 database 'stars': {e}")
            logging.error("Make sure PostgreSQL is running and RTS2 database is set up")
            return 1

    # Create device
    device = app.create_device(GrbDaemon)

    # Register additional command handlers
    grb_commands = GrbCommands(device)
    device.network.command_registry.register_handler(grb_commands)

    logging.info("Starting RTS2 GRB Daemon with GCN Kafka interface")
    logging.info(f"GCN Domain: {args.gcn_domain}")
    logging.info(f"Client ID: {args.gcn_client_id}")
    logging.info("Database: PostgreSQL 'stars' on localhost")

    if args.disable_grbs:
        logging.info("GRB observations DISABLED - monitoring only")

    if args.queue_to:
        logging.info(f"GRBs will be queued to: {args.queue_to}")

    if args.add_exec:
        logging.info(f"External script: {args.add_exec}")

    # Run application
    try:
        app.run()
        return 0
    except KeyboardInterrupt:
        logging.info("Shutting down GRB daemon")
        return 0
    except Exception as e:
        logging.error(f"Fatal error in GRB daemon: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
