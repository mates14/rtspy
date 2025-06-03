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
import re
import math
from typing import Dict, Optional, Any, Callable
from datetime import datetime
import psycopg2

try:
    from gcn_kafka import Consumer
    GCN_KAFKA_AVAILABLE = True
except ImportError:
    logging.warning("gcn-kafka not available. Install with: pip install gcn-kafka")
    GCN_KAFKA_AVAILABLE = False

from constants import ConnectionState, DeviceType
from device import Device
from config import DeviceConfig
from value import (ValueBool, ValueString, ValueInteger, ValueTime, ValueDouble, ValueRaDec)
from app import App
from voevent import VoEventParser, GrbTarget

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
                    'gcn.classic.voevent.FERMI_GBM_POS_TEST',
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


class GrbDaemon(Device, DeviceConfig):
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
        #config.add_argument('--gcn-client-id', help='GCN Kafka client ID', required=True)
        #config.add_argument('--gcn-client-secret', help='GCN Kafka client secret', required=True)
        config.add_argument('--gcn-client-id', help='GCN Kafka client ID', default='ibkn5j8ocfhhdftq6feedfmdv')
        config.add_argument('--gcn-client-secret', help='GCN Kafka client secret', default='rh3v8p9sqq9o0q8goqufdl2449eq7fl37hf74096c5km9l6cjm')

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

        # Add thread safety for statistics
        self._stats_lock = threading.Lock()
        self._processing_times = []
        self._packet_times = []
        self._recent_grb_list = []
        self._last_daily_reset = time.time()

        # GCN Kafka consumer
        self.gcn_consumer = None

        # Current GRB tracking
        self.current_grb = None
        self.grb_targets = {}  # target_id -> GrbTarget
        self.next_target_id = 1

        # Statistics
        self.last_packet_time = 0.0

        # Create RTS2 values for monitoring
        self._create_values()
    
        # keep record of centrald/system state
        self.system_state = None
        self.trigger_ready = False

        # Set initial state
        self.set_state(self.STATE_IDLE, "GRB daemon initializing")
        # System state monitoring
        self.system_state_required = 0x03    # ON (0x0.) + NIGHT (0x.3)

    def apply_config(self, config: Dict[str, Any]):
        """Apply GRB-specific configuration."""
        super().apply_config(config)

        # Apply GCN configuration
        self.gcn_client_id.value = config.get('gcn_client_id', '')
        self.gcn_client_secret = config.get('gcn_client_secret', '')

        # Apply GRB configuration
        self.grb_enabled.value = not config.get('disable_grbs', False)
        self.create_disabled.value = config.get('create_disabled', False)
        self.queue_name.value = config.get('queue_to','grb')
        # self.add_exec.value = config.get('add_exec')
        # self.exec_followups.value = config.get('exec_followups', False)
        self.record_not_visible.value = config.get('not_visible', True)
#        self.record_only_visible_tonight.value = config.get('only_visible_tonight', False)
        self.min_altitude.value = config.get('min_altitude', 0.0)

    def _create_values(self):
        """Create RTS2 values for comprehensive monitoring and control."""

        # === CONTROL VALUES ===
        self.grb_enabled = ValueBool("enabled", "GRB reception enabled", writable=True)
        self.create_disabled = ValueBool("create_disabled", "Create GRBs disabled for auto-observation", writable=True)
        self.record_not_visible = ValueBool("not_visible", "Record GRBs not visible from location", writable=True)
        self.only_visible_tonight = ValueBool("only_visible_tonight", "Record only GRBs visible tonight", writable=True)
        self.min_altitude = ValueDouble("min_altitude", "Minimum GRB altitude for visibility", writable=True)
        self.queue_name = ValueString("queue_name", "Queue for triggers", writable=True)

        # === CONNECTION STATUS ===
        self.gcn_connected = ValueBool("gcn_connected", "GCN Kafka connection status")
        self.gcn_client_id = ValueString("gcn_client_id", "GCN client ID")
        self.connection_time = ValueTime("connection_time", "Time of last GCN connection")
        self.last_heartbeat = ValueTime("last_heartbeat", "Last GCN heartbeat/activity")

        # === PACKET STATISTICS ===
        self.packets_received = ValueInteger("packets_received", "Total GCN packets received", initial=0)
        self.packets_today = ValueInteger("packets_today", "GCN packets received today", initial=0)
        self.grbs_processed = ValueInteger("grbs_processed", "Total GRBs processed", initial=0)
        self.grbs_today = ValueInteger("grbs_today", "GRBs processed today", initial=0)
        self.last_packet = ValueTime("last_packet", "Time of last GCN packet")
        self.packet_rate = ValueDouble("packet_rate", "Recent packet rate [packets/hour]")

        # === MISSION-SPECIFIC COUNTERS ===
        self.fermi_alerts = ValueInteger("fermi_alerts", "Fermi GBM alerts processed", initial=0)
        self.swift_alerts = ValueInteger("swift_alerts", "Swift BAT alerts processed", initial=0)
        self.maxi_alerts = ValueInteger("maxi_alerts", "MAXI alerts processed", initial=0)
        self.icecube_alerts = ValueInteger("icecube_alerts", "IceCube alerts processed", initial=0)
        self.svom_alerts = ValueInteger("svom_alerts", "SVOM alerts processed", initial=0)
        self.other_alerts = ValueInteger("other_alerts", "Other mission alerts", initial=0)

        # === LAST GRB INFORMATION ===
        self.last_target = ValueString("last_target", "Name of last GRB target")
        self.last_target_id = ValueInteger("last_target_id", "ID of last GRB target")
        self.last_target_time = ValueTime("last_target_time", "Time of last target creation")
        self.last_target_type = ValueInteger("last_target_type", "Type of last target")
        self.last_target_radec = ValueRaDec("last_target_radec", "Coordinates (J2000) of last GRB")
        self.last_target_errorbox = ValueDouble("last_target_errorbox", "Error box of last target (degrees)")
        self.last_mission = ValueString("last_mission", "Mission of last GRB")

        # === RECENT ACTIVITY ===
        self.recent_grbs = ValueString("recent_grbs", "Recent GRB triggers (last 5)")
        self.targets_created_today = ValueInteger("targets_created_today", "New targets created today", initial=0)
        self.targets_updated_today = ValueInteger("targets_updated_today", "Existing targets updated today", initial=0)
        self.observations_triggered = ValueInteger("observations_triggered", "Observations triggered today", initial=0)

        # === LAST ALERTS BY MISSION ===
        self.last_fermi_time = ValueTime("last_fermi", "Time of last Fermi alert")
        self.last_fermi_trigger = ValueString("last_fermi_trigger", "Last Fermi trigger ID")
        self.last_fermi_coords = ValueRaDec("last_fermi_coords", "Last Fermi position")

        self.last_swift_time = ValueTime("last_swift", "Time of last Swift alert")
        self.last_swift_trigger = ValueString("last_swift_trigger", "Last Swift trigger ID")
        self.last_swift_coords = ValueRaDec("last_swift_coords", "Last Swift position")

        self.last_maxi_time = ValueTime("last_maxi", "Time of last MAXI alert")
        self.last_maxi_trigger = ValueString("last_maxi_trigger", "Last MAXI trigger ID")
        self.last_maxi_coords = ValueRaDec("last_maxi_coords", "Last MAXI position")

        self.last_icecube_time = ValueTime("last_icecube", "Time of last IceCube alert")
        self.last_icecube_trigger = ValueString("last_icecube_trigger", "Last IceCube event ID")
        self.last_icecube_coords = ValueRaDec("last_icecube_coords", "Last IceCube position")

        # === ERROR TRACKING ===
        self.parse_errors = ValueInteger("parse_errors", "GCN message parse errors", initial=0)
        self.database_errors = ValueInteger("database_errors", "Database operation errors", initial=0)
        self.last_error = ValueString("last_error", "Last error message")
        self.last_error_time = ValueTime("last_error_time", "Time of last error")

        # === PERFORMANCE METRICS ===
        self.processing_time_avg = ValueDouble("processing_time_avg", "Average message processing time [ms]")
        self.database_time_avg = ValueDouble("database_time_avg", "Average database operation time [ms]")
        self.queue_size = ValueInteger("queue_size", "Current message queue size")

        # === OPERATIONAL STATUS ===
        self.status_message = ValueString("status", "Current daemon status")
        self.uptime_hours = ValueDouble("uptime_hours", "Daemon uptime in hours")
        self.topics_subscribed = ValueInteger("topics_subscribed", "Number of GCN topics subscribed", initial=0)

        # Initialize daily counters
        self._reset_daily_counters()
        self._mission_counters = {
            'FERMI': 0, 'SWIFT': 0, 'MAXI': 0, 'ICECUBE': 0, 'SVOM': 0, 'OTHER': 0
        }
        self._recent_grb_list = []
        self._processing_times = []
        self._last_daily_reset = time.time()


    def _reset_daily_counters(self):
        """Reset daily counters at midnight or startup."""
        self.packets_today.value = 0
        self.grbs_today.value = 0
        self.targets_created_today.value = 0
        self.targets_updated_today.value = 0
        self.observations_triggered.value = 0


    def _update_mission_statistics(self, mission: str, grb_info: 'GrbTarget'):
        """Update mission-specific statistics."""
        current_time = time.time()

        # Update mission counters
        if mission == 'FERMI':
            self.fermi_alerts.value += 1
            self.last_fermi_time.value = current_time
            self.last_fermi_trigger.value = grb_info.grb_id
            if self._is_valid_coordinates(grb_info.ra, grb_info.dec):
                self.last_fermi_coords.value = (grb_info.ra, grb_info.dec)

        elif mission == 'SWIFT':
            self.swift_alerts.value += 1
            self.last_swift_time.value = current_time
            self.last_swift_trigger.value = grb_info.grb_id
            if self._is_valid_coordinates(grb_info.ra, grb_info.dec):
                self.last_swift_coords.value = (grb_info.ra, grb_info.dec)

        elif mission == 'MAXI':
            self.maxi_alerts.value += 1
            self.last_maxi_time.value = current_time
            self.last_maxi_trigger.value = grb_info.grb_id
            if self._is_valid_coordinates(grb_info.ra, grb_info.dec):
                self.last_maxi_coords.value = (grb_info.ra, grb_info.dec)

        elif mission == 'ICECUBE':
            self.icecube_alerts.value += 1
            self.last_icecube_time.value = current_time
            self.last_icecube_trigger.value = grb_info.grb_id
            if self._is_valid_coordinates(grb_info.ra, grb_info.dec):
                self.last_icecube_coords.value = (grb_info.ra, grb_info.dec)

        else:
            self.other_alerts.value += 1

    def _update_recent_grbs(self, grb_info: 'GrbTarget'):
        """Update recent GRBs list with thread safety."""
        grb_summary = f"{grb_info.grb_id}({grb_info.mission})"

        with self._stats_lock:
            self._recent_grb_list.append(grb_summary)

            # Keep only last 5
            if len(self._recent_grb_list) > 5:
                self._recent_grb_list = self._recent_grb_list[-5:]

            self.recent_grbs.value = ", ".join(self._recent_grb_list)

    # FIXED: Enhanced error tracking with thread safety
    def _track_processing_time(self, start_time: float):
        """Track message processing performance with thread safety."""
        processing_time_ms = (time.time() - start_time) * 1000

        with self._stats_lock:
            self._processing_times.append(processing_time_ms)

            # Keep only last 100 measurements
            if len(self._processing_times) > 100:
                self._processing_times = self._processing_times[-100:]

            # Update average
            if self._processing_times:
                self.processing_time_avg.value = sum(self._processing_times) / len(self._processing_times)

    def _calculate_packet_rate(self):
        """Calculate recent packet rate."""
        current_time = time.time()

        # Calculate packets per hour based on recent activity
        if hasattr(self, '_packet_times'):
            # Remove packets older than 1 hour
            hour_ago = current_time - 3600
            self._packet_times = [t for t in self._packet_times if t > hour_ago]
            self.packet_rate.value = len(self._packet_times)
        else:
            self._packet_times = []


    def _check_daily_reset(self):
        """Check if we need to reset daily counters."""
        current_time = time.time()
        current_day = int(current_time / 86400)  # Days since epoch
        last_day = int(self._last_daily_reset / 86400)

        if current_day > last_day:
            logging.info("Resetting daily counters for new day")
            self._reset_daily_counters()
            self._last_daily_reset = current_time


    def _log_error(self, error_type: str, error_msg: str):
        """Log and track errors."""
        current_time = time.time()

        if error_type == "parse":
            self.parse_errors.value += 1
        elif error_type == "database":
            self.database_errors.value += 1

        self.last_error.value = f"{error_type}: {error_msg}"
        self.last_error_time.value = current_time

        logging.error(f"GRB daemon {error_type} error: {error_msg}")


    # Enhanced info() method
    def info(self):
        """Update device information with comprehensive status."""
        super().info()

        current_time = time.time()

        # Check daily reset
        self._check_daily_reset()

        # Update connection status
        if self.gcn_consumer:
            self.gcn_connected.value = getattr(self.gcn_consumer, 'running', False)
            if hasattr(self.gcn_consumer, 'last_message_time'):
                self.last_heartbeat.value = self.gcn_consumer.last_message_time

        if self.last_packet_time > 0:
            self.last_packet.value = self.last_packet_time

        # Update packet rate
        self._calculate_packet_rate()

        # Update uptime
        if hasattr(self, 'start_time'):
            uptime_seconds = current_time - self.start_time
            self.uptime_hours.value = uptime_seconds / 3600.0

        # Update current GRB information
        if self.current_grb:
            self.last_target.value = self.current_grb.grb_id
            self.last_target_id.value = self.current_grb.target_id
            self.last_target_time.value = self.current_grb.detection_time
            self.last_target_type.value = self.current_grb.grb_type
            self.last_mission.value = self.current_grb.mission

            if self._is_valid_coordinates(self.current_grb.ra, self.current_grb.dec):
                self.last_target_radec.value = (self.current_grb.ra, self.current_grb.dec)

            if not math.isnan(self.current_grb.error_box):
                self.last_target_errorbox.value = self.current_grb.error_box

        # Update status message
        if self.gcn_connected.value:
            hours_since_packet = (current_time - self.last_packet_time) / 3600 if self.last_packet_time > 0 else 999
            if hours_since_packet < 1:
                status = "Active - receiving packets"
            elif hours_since_packet < 6:
                status = f"Quiet - {hours_since_packet:.1f}h since last packet"
            else:
                status = f"Stale - {hours_since_packet:.1f}h since last packet"
        else:
            status = "Disconnected from GCN"

        self.status_message.value = status

    def start(self):
        """Start the GRB daemon."""
        super().start()

        # Record start time for uptime calculation
        self.start_time = time.time()

        # Register interest in system state
        logging.info("Monitoring centrald system state for GRB readiness")
        self.network.register_state_interest("centrald", self._on_system_state_changed)

        # Validate configuration
        if not self.gcn_client_id.value or not self.gcn_client_secret:
            logging.error("GCN client ID and secret must be provided")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, "Missing GCN credentials")
            return

        logging.info(f"GCN Client ID: {self.gcn_client_id.value}")

        # Initialize GCN Kafka consumer
        self.gcn_consumer = GcnKafkaConsumer(
            client_id=self.gcn_client_id.value,
            client_secret=self.gcn_client_secret,
        )
        self.gcn_consumer.set_message_callback(self._on_gcn_message)

        # Start consumer
        if self.gcn_consumer.start():
            logging.info("GRB daemon started, connected to GCN")
            self.set_ready("Connected to GCN Kafka")

            # Update connection status
            self.gcn_connected.value = True
            self.connection_time.value = time.time()
        else:
            logging.error("Failed to start GCN consumer")
            self.set_state(self.STATE_IDLE | self.ERROR_HW, "Failed to connect to GCN")

        if hasattr(self, 'gcn_consumer') and self.gcn_consumer:
            # Count subscribed topics
            if hasattr(self.gcn_consumer, 'topics'):
                self.topics_subscribed.value = len(self.gcn_consumer.topics)

    def _on_system_state_changed(self, dev, state, bop, msg):
        """Handle system state mask changes from centrald."""
        try:
            if state != self.system_state:
                self.system_state = state
                logging.info(f"System state changed to 0x{state:02x}")

                if state & 0xff == self.system_state_required:
                    logging.info("System ready for GRB observations (ON & NIGHT)")
                    self.trigger_ready = True
                else:
                    logging.info(f"System not ready for triggers (0x{state:02x})")
                    self.trigger_ready = False

        except (ValueError, TypeError):
            logging.warning(f"Could not parse state mask: {state_mask_value}")

    def stop(self):
        """Stop the GRB daemon."""
        if self.gcn_consumer:
            self.gcn_consumer.stop()
        super().stop()

    # Enhanced message processing with statistics
    def _on_gcn_message(self, topic: str, message: str):
        """Enhanced message handler with comprehensive tracking."""
        start_time = time.time()

        try:
            # Thread-safe packet tracking
            with self._stats_lock:
                self.packets_received.value += 1
                self.packets_today.value += 1
                self.last_packet_time = time.time()

                # Track packet times for rate calculation
                self._packet_times.append(self.last_packet_time)
                # Keep only last hour
                hour_ago = self.last_packet_time - 3600
                self._packet_times = [t for t in self._packet_times if t > hour_ago]

            logging.debug(f"Processing GCN message from {topic}")

            # Update last heartbeat
            self.last_heartbeat.value = self.last_packet_time

            # Try to parse as GRB/transient, skip if not parseable
            grb_info = self._parse_grb_notice(topic, message)

            if grb_info:
                # Got valid transient info - process it
                self._process_grb_info(grb_info)
            else:
                # Not a transient alert - log and skip
                logging.debug(f"Skipping non-transient message from {topic}")

            # Track processing time
            self._track_processing_time(start_time)

        except Exception as e:
            self._log_error("parse", str(e))
            logging.error(f"Error processing GCN message from {topic}: {e}")
            logging.exception("Detailed traceback:")

    def _process_grb_info(self, grb_info: 'GrbTarget'):
        """Process parsed GRB information - FIXED VERSION."""
        try:
            # Update mission statistics
            self._update_mission_statistics(grb_info.mission, grb_info)
            self._update_recent_grbs(grb_info)

            logging.info(f"Received transient alert: {grb_info.grb_id} "
                        f"from {grb_info.mission}")

            # Log coordinates if valid
            if self._is_valid_coordinates(grb_info.ra, grb_info.dec):
                logging.info(f"  Position: RA={grb_info.ra:.3f}, Dec={grb_info.dec:.3f}")
                if not math.isnan(grb_info.error_box):
                    logging.info(f"  Error box: {grb_info.error_box:.3f} degrees")
            else:
                logging.info(f"  No valid coordinates (RA={grb_info.ra:.3f}, Dec={grb_info.dec:.3f})")

            # Add to database with proper error handling
            db_start_time = time.time()
            target_id = self._add_grb_to_database(grb_info)
            db_time_ms = (time.time() - db_start_time) * 1000
            self.database_time_avg.value = db_time_ms

            if target_id:
                grb_info.target_id = target_id

                # Store GRB information
                self.grb_targets[target_id] = grb_info
                self.current_grb = grb_info

                with self._stats_lock:
                    self.grbs_processed.value += 1
                    self.grbs_today.value += 1

                # Only trigger observations for valid coordinates
                if not self._is_valid_coordinates(grb_info.ra, grb_info.dec):
                    logging.info(f"Skipping observation trigger for {grb_info.grb_id} - no valid coordinates")
                    return

                # Check visibility if filtering enabled
                if not self._check_visibility_constraints(grb_info):
                    logging.info(f"Transient {grb_info.grb_id} not visible, skipping observation")
                    return

                # Trigger observation if enabled
                if self.grb_enabled.value and grb_info.is_grb:
                    self._trigger_grb_observation(target_id)
                    with self._stats_lock:
                        self.observations_triggered.value += 1
                else:
                    logging.info(f"Transient observation disabled: {grb_info.grb_id}")
            else:
                logging.info(f"Transient {grb_info.grb_id} not added to database")

        except Exception as e:
            self._log_error("grb_processing", str(e))
            logging.error(f"Error processing transient: {e}")
            logging.exception("Detailed error:")

    def _add_grb_to_database(self, grb: GrbTarget) -> Optional[int]:
        """
        Add GRB target to RTS2 PostgreSQL database with time-based deduplication.

        Logic:
        1. Check for exact trigger ID match first
        2. If not found, look for GRBs within 15-minute time window
        3. If found, verify coordinates are consistent (rare multi-GRB check)
        4. Otherwise create new target
        """
        try:
            # Connect to RTS2 PostgreSQL database
            conn = psycopg2.connect(
                host="localhost",
                database="stars",  # Default RTS2 database name
                user="mates",
                password="pasewcic25"  # Assumes peer authentication or configured password
            )
            cursor = conn.cursor()

            # Convert string grb_id to integer for database
            grb_id_int = self._convert_grb_id_to_int(grb.grb_id)

            logging.debug(f"Processing GRB ID: {grb.grb_id} -> {grb_id_int}")

            # Step 1: Check for existing GRB by exact trigger ID
            cursor.execute("""
                SELECT g.tar_id, g.grb_errorbox, g.grb_ra, g.grb_dec, t.tar_name
                FROM grb g
                JOIN targets t ON g.tar_id = t.tar_id
                WHERE g.grb_id = %s
                ORDER BY g.grb_last_update DESC
                LIMIT 1
            """, (grb_id_int,))

            existing_exact = cursor.fetchone()

            if existing_exact:
                logging.debug(f"Found existing trigger {grb.grb_id}: {existing_exact[4]}")
                return self._update_existing_grb_target(cursor, conn, grb, grb_id_int, existing_exact)

            # Step 2: Look for GRBs within 15-minute time window
            time_window = 900  # 15 minutes in seconds

            logging.debug(f"No exact trigger match, searching for GRBs within 15 minutes of {grb.detection_time}")

            cursor.execute("""
                SELECT g.tar_id, g.grb_id, g.grb_ra, g.grb_dec, g.grb_errorbox,
                       g.grb_date, t.tar_name, EXTRACT(EPOCH FROM g.grb_date) as epoch_time
                FROM grb g
                JOIN targets t ON g.tar_id = t.tar_id
                WHERE ABS(EXTRACT(EPOCH FROM g.grb_date) - %s) < %s
                  AND g.grb_is_grb = true
                ORDER BY ABS(EXTRACT(EPOCH FROM g.grb_date) - %s)
            """, (grb.detection_time, time_window, grb.detection_time))

            time_candidates = cursor.fetchall()

            if not time_candidates:
                logging.debug("No GRBs found within 15-minute window")
                return self._create_new_grb_if_valid(cursor, conn, grb, grb_id_int)

            logging.debug(f"Found {len(time_candidates)} GRBs within 15-minute window")

            # Step 3: Check if any candidates are position-compatible
            # (This is the rare multi-GRB safety check)
            for candidate in time_candidates:
                (cand_tar_id, cand_grb_id, cand_ra, cand_dec, cand_errorbox,
                 cand_date, cand_name, cand_epoch) = candidate

                time_diff = abs(grb.detection_time - cand_epoch)
                logging.debug(f"Candidate {cand_name} (trigger {cand_grb_id}): {time_diff:.1f}s apart")

                # If both have valid coordinates, check compatibility
                if (self._is_valid_coordinates(grb.ra, grb.dec) and
                    self._is_valid_coordinates(cand_ra, cand_dec)):

                    # Calculate position difference with proper error combination
                    compatible = self._are_positions_compatible(
                        grb.ra, grb.dec, grb.error_box,
                        cand_ra, cand_dec, cand_errorbox
                    )

                    if compatible:
                        logging.info(f"GRB {grb.grb_id} matches existing target {cand_name} "
                                   f"(time: {time_diff:.1f}s, positions compatible)")
                        return self._link_to_existing_target(cursor, conn, grb, grb_id_int, candidate)
                    else:
                        logging.warning(f"Potential multi-GRB situation! {grb.grb_id} within {time_diff:.1f}s "
                                      f"of {cand_name} but positions incompatible. Creating separate target.")

                elif not self._is_valid_coordinates(grb.ra, grb.dec):
                    # New GRB has no valid coordinates, assume it's related to first time match
                    logging.info(f"GRB {grb.grb_id} (no coordinates) matches {cand_name} by time ({time_diff:.1f}s)")
                    return self._link_to_existing_target(cursor, conn, grb, grb_id_int, candidate)

                elif not self._is_valid_coordinates(cand_ra, cand_dec):
                    # Candidate has no valid coordinates, link if this one does
                    if self._is_valid_coordinates(grb.ra, grb.dec):
                        logging.info(f"GRB {grb.grb_id} provides coordinates for existing target {cand_name}")
                        return self._link_to_existing_target(cursor, conn, grb, grb_id_int, candidate)

            # Step 4: No compatible candidates found, create new target
            logging.info(f"No compatible targets found within 15 minutes, creating new target for {grb.grb_id}")
            return self._create_new_grb_if_valid(cursor, conn, grb, grb_id_int)

        except Exception as e:
            logging.error(f"Error adding GRB to database: {e}")
            logging.exception("Detailed error:")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return None
        finally:
            # FIXED: Always close connection
            if conn:
                try:
                    conn.close()
                except:
                    pass

    def _convert_grb_id_to_int(self, grb_id: str) -> int:
        """FIXED: Safely convert GRB ID to integer."""
        try:
            return int(grb_id)
        except (ValueError, TypeError):
            # If grb_id is not numeric, create a stable hash
            return abs(hash(grb_id)) % 2147483647

    def _are_positions_compatible(self, ra1, dec1, err1, ra2, dec2, err2):
        """
        Check if two positions are compatible within their combined error boxes.

        Uses proper error combination: total_error = sqrt(err1² + err2²)
        But also handles RA/Dec errors separately if needed.
        """
        # Calculate angular separation
        angular_sep = self._calculate_angular_separation(ra1, dec1, ra2, dec2)

        # Combine errors (assume circular for simplicity)
        # In practice, you might want to handle RA/Dec errors separately
        error1 = err1 if not math.isnan(err1) else 10.0  # Default large error for GBM
        error2 = err2 if not math.isnan(err2) else 10.0

        # Combined error using RSS (Root Sum of Squares)
        combined_error = math.sqrt(error1**2 + error2**2)

        # Add some tolerance for systematic errors
        tolerance = 1.0  # Additional 1 degree tolerance
        total_allowed_error = combined_error + tolerance

        compatible = angular_sep <= total_allowed_error

        logging.debug(f"Position compatibility check:")
        logging.debug(f"  Pos1: ({ra1:.3f}, {dec1:.3f}) ± {error1:.3f}°")
        logging.debug(f"  Pos2: ({ra2:.3f}, {dec2:.3f}) ± {error2:.3f}°")
        logging.debug(f"  Separation: {angular_sep:.3f}°")
        logging.debug(f"  Combined error: {combined_error:.3f}° + {tolerance:.3f}° = {total_allowed_error:.3f}°")
        logging.debug(f"  Compatible: {compatible}")

        return compatible


    def _update_existing_grb_target(self, cursor, conn, grb: GrbTarget, grb_id_int: int, existing):
        """Update existing target for the same trigger ID."""
        (existing_tar_id, existing_error, existing_ra, existing_dec, existing_name) = existing

        logging.info(f"Updating existing target {existing_name} (ID: {existing_tar_id}) for trigger {grb.grb_id}")

        # Determine if we should update the target position
        should_update_position = False
        update_reason = ""

        if not self._is_valid_coordinates(existing_ra, existing_dec):
            if self._is_valid_coordinates(grb.ra, grb.dec):
                should_update_position = True
                update_reason = "adding first valid coordinates"
        elif self._is_valid_coordinates(grb.ra, grb.dec):
            if (existing_error is None or math.isnan(existing_error) or
                (not math.isnan(grb.error_box) and grb.error_box < existing_error)):
                should_update_position = True
                update_reason = f"better accuracy: {existing_error:.3f}° -> {grb.error_box:.3f}°"

        # Update target position if warranted
        if should_update_position:
            cursor.execute("""
                UPDATE targets
                SET tar_ra = %s, tar_dec = %s
                WHERE tar_id = %s
            """, (grb.ra, grb.dec, existing_tar_id))
            logging.info(f"  Updated target position: {update_reason}")

        # Always insert new GRB record for this alert
        cursor.execute("""
            INSERT INTO grb (
                tar_id, grb_id, grb_seqn, grb_type, grb_ra, grb_dec,
                grb_is_grb, grb_date, grb_last_update, grb_errorbox
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s)
        """, (existing_tar_id, grb_id_int, grb.sequence_num, grb.grb_type,
              grb.ra, grb.dec, grb.is_grb, grb.detection_time,
              time.time(), grb.error_box))

        # Add raw packet data
        self._add_gcn_raw_packet(cursor, grb, grb_id_int)

        conn.commit()
        conn.close()
        return existing_tar_id


    def _link_to_existing_target(self, cursor, conn, grb: GrbTarget, grb_id_int: int, candidate):
        """Link new GRB alert to existing target based on time/position match."""
        (cand_tar_id, cand_grb_id, cand_ra, cand_dec, cand_errorbox,
         cand_date, cand_name, cand_epoch) = candidate

        time_diff = abs(grb.detection_time - cand_epoch)
        logging.info(f"Linking GRB {grb.grb_id} to existing target {cand_name} "
                   f"(time difference: {time_diff:.1f}s)")

        # Determine if we should update the target position
        should_update_position = False
        update_reason = ""

        if not self._is_valid_coordinates(cand_ra, cand_dec):
            if self._is_valid_coordinates(grb.ra, grb.dec):
                should_update_position = True
                update_reason = "adding coordinates to target without position"
        elif self._is_valid_coordinates(grb.ra, grb.dec):
            if (cand_errorbox is None or math.isnan(cand_errorbox) or
                (not math.isnan(grb.error_box) and grb.error_box < cand_errorbox)):
                should_update_position = True
                update_reason = f"better accuracy: {cand_errorbox:.3f}° -> {grb.error_box:.3f}°"

        # Update target position if warranted
        if should_update_position:
            cursor.execute("""
                UPDATE targets
                SET tar_ra = %s, tar_dec = %s
                WHERE tar_id = %s
            """, (grb.ra, grb.dec, cand_tar_id))
            logging.info(f"  Updated target position: {update_reason}")

        # Insert new GRB record pointing to the existing target
        cursor.execute("""
            INSERT INTO grb (
                tar_id, grb_id, grb_seqn, grb_type, grb_ra, grb_dec,
                grb_is_grb, grb_date, grb_last_update, grb_errorbox
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s), %s)
        """, (cand_tar_id, grb_id_int, grb.sequence_num, grb.grb_type,
              grb.ra, grb.dec, grb.is_grb, grb.detection_time,
              time.time(), grb.error_box))

        # Add raw packet data
        self._add_gcn_raw_packet(cursor, grb, grb_id_int)

        conn.commit()
        conn.close()
        return cand_tar_id


    def _create_new_grb_if_valid(self, cursor, conn, grb: GrbTarget, grb_id_int: int):
        """Create new GRB target only if coordinates are valid."""

        # Skip creation if invalid coordinates
        if not self._is_valid_coordinates(grb.ra, grb.dec):
            logging.info(f"Skipping GRB {grb.grb_id} with invalid coordinates "
                        f"(RA={grb.ra:.3f}, Dec={grb.dec:.3f}). "
                        f"Likely initial trigger alert without position.")
            conn.close()
            return None

        # Create new target
        return self._create_new_grb_target(cursor, conn, grb, grb_id_int)


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
        tar_enabled = not self.create_disabled.value

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


    def _is_valid_coordinates(self, ra, dec):
        """Check if coordinates are valid (not NaN, None, not 0,0, within range)."""
        if ra is None or dec is None:
            return False
        if math.isnan(ra) or math.isnan(dec):
            return False
        # Invalid/placeholder coordinates for initial triggers
        if ra == 0.0 and dec == 0.0:
            return False
        if not (0 <= ra <= 360):
            return False
        if not (-90 <= dec <= 90):
            return False
        return True

#    def _parse_coordinates_safely(self, ra_str, dec_str):
#        """FIXED: Safe coordinate parsing with validation."""
#        try:
#            ra = float(ra_str)
#            dec = float(dec_str)
#
#            # Validate ranges
#            if not (0 <= ra <= 360):
#                logging.warning(f"Invalid RA: {ra}, setting to NaN")
#                ra = float('nan')
#            if not (-90 <= dec <= 90):
#                logging.warning(f"Invalid Dec: {dec}, setting to NaN")
#                dec = float('nan')
#
#            return ra, dec
#        except (ValueError, TypeError):
#            logging.warning(f"Could not parse coordinates: '{ra_str}', '{dec_str}'")
#            return float('nan'), float('nan')

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
        if self.record_not_visible.value:
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

        # Check altitude constraint
        if not math.isnan(grb.dec) and not math.isnan(self.min_altitude.value):
            pass
            # Simplified altitude check - would need proper observatory coordinates
            # and current time for full calculation

        # Check "only visible tonight" constraint
        if self.only_visible_tonight.value:
            # This would require detailed rise/set calculations
            # For now, just do a simplified check
            # current_time = time.time()
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
        Parse any transient notice from GCN - ROBUST VERSION.

        Returns:
            GrbTarget for any transient (GRB, neutrino, whatever)
            or None if not a transient.
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
                    # Use the robust XML parser
                    parser = VoEventParser()
                    grb = parser.parse_voevent(message, grb)
                    logging.debug(f"Successfully parsed XML for {grb.mission} trigger {grb.grb_id}")
                    return grb
                except Exception as e:
                    logging.warning(f"XML parsing failed for {topic}, trying text fallback: {e}")

            # Fallback to enhanced text parsing
            grb = self._parse_text_format(message, grb)

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

    def _parse_text_format(self, message: str, grb: GrbTarget) -> GrbTarget:
        """Enhanced text format parser with better regex patterns."""
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

            # Enhanced coordinate extraction with multiple formats
            coordinate_patterns = [
                # Standard RA/DEC format
                (r'RA:\s*([\d.]+)', r'DEC?:\s*([-+]?[\d.]+)'),
                # Parameter format
                (r'RA["\s]*[:=]\s*["\']?([\d.]+)', r'Dec["\s]*[:=]\s*["\']?([-+]?[\d.]+)'),
                # XML-like format in text
                (r'<C1>([\d.]+)</C1>', r'<C2>([-+]?[\d.]+)</C2>'),
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

            # Check for test/retraction indicators
            test_indicators = ['TEST', 'RETRACTION', 'PRELIMINARY', 'SIMULATION']
            if any(indicator in message.upper() for indicator in test_indicators):
                grb.is_grb = False
                logging.debug("Detected test/retraction/preliminary event")

            return grb

        except Exception as e:
            logging.error(f"Error in enhanced text parsing: {e}")
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

            if not self.trigger_ready:
                logging.info(f"System not ready for immediate GRB observation (state=0x{self.system_state:02x})")
                logging.info("GRB will be discovered by scheduler for time-critical scheduling")
                # Do not queue - let the scheduler handle it
                return

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

            #elif self.queue_name.value:
            #    # Queue GRB for selector
            #    self._queue_grb_observation(target_id)

            else:
                logging.warning(f"No executor available and no queue specified for GRB {target_id}")

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
                cmd = f"queue_now_once {self.queue_name.value} {target_id}"
                conn.send_command(cmd, self._on_queue_grb_result)
                logging.info(f"Queued GRB {target_id} to selector {conn.name}")

        except Exception as e:
            logging.error(f"Error queuing GRB observation: {e}")

    def _on_execute_grb_result(self, conn, success, code, message):
        """Handle result from executor GRB command."""
        if success:
            logging.info(f"GRB execution successful: {message}")
            with self._stats_lock:
                self.observations_triggered.value += 1
        else:
            logging.error(f"GRB execution failed: {message}")
            # Try queuing if execution failed and queue is configured
            #if self.queue_name.value and self.current_grb:
            #    self._queue_grb_observation(self.current_grb.target_id)

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
#        self.create_disabled.value = new_value
        logging.info(f"Create disabled set to: {new_value}")

    def on_not_visible_changed(self, old_value, new_value):
        """Handle changes to record not visible state."""
#        self.record_not_visible = new_value
        logging.info(f"Record not visible set to: {new_value}")

#    def on_only_visible_tonight_changed(self, old_value, new_value):
#        """Handle changes to only visible tonight state."""
#        self.record_only_visible_tonight = new_value
#        logging.info(f"Only visible tonight set to: {new_value}")

    def on_min_grb_altitude_changed(self, old_value, new_value):
        """Handle changes to minimum GRB altitude."""
#        self.min_altitude = new_value
        logging.info(f"Minimum GRB altitude set to: {self.min_altitude.value} degrees")


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

    # Test database connection
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="stars",
            user="mates",
            password="pasewcic25"
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
    logging.info(f"Client ID: {args.gcn_client_id}")
    logging.info("Database: PostgreSQL 'stars' on localhost")

    if args.queue_to:
        logging.info(f"GRBs will be queued to: {args.queue_to}")

#    if args.add_exec:
#        logging.info(f"External script: {args.add_exec}")

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
