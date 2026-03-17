#!/usr/bin/python3

import time
import logging
import threading
import os
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

from rtspy.core.value import ValueDouble, ValueInteger, ValueBool
from rtspy.core.config import DeviceConfig


class DeviceCaringMixin:
    """
    Mixin class that provides continuous device monitoring and care loop functionality.
    
    This mixin provides infrastructure for devices that need proactive monitoring
    rather than just reactive command processing. It handles thread management,
    timing, error recovery, and provides clean interfaces for device-specific
    monitoring logic.
    
    Devices that need continuous monitoring inherit from both their base class
    and this mixin, then implement device_care_update() for their specific logic.
    """

    def setup_caring_config(self, config):
        """Register caring loop configuration arguments."""
        config.add_argument('--caring-interval', type=float, default=0.1,
                          help='Caring loop update interval in seconds', section='caring')
        config.add_argument('--caring-enabled', action='store_true', default=True,
                          help='Enable caring loop', section='caring')
        config.add_argument('--caring-error-backoff', type=float, default=1.0,
                          help='Error backoff time in seconds', section='caring')
        config.add_argument('--caring-freq-update', type=float, default=5.0,
                          help='Frequency value update interval in seconds', section='caring')

    def apply_caring_config(self, config: Dict[str, Any]):
        """Apply caring loop configuration."""
        self.caring_interval = config.get('caring_interval', 0.1)
        self.caring_enabled_config = config.get('caring_enabled', True)
        self.caring_error_backoff = config.get('caring_error_backoff', 1.0)
        self.caring_freq_update_interval = config.get('caring_freq_update', 5.0)

    def __init__(self, *args, **kwargs):
        """Initialize caring mixin."""
        super().__init__(*args, **kwargs)
        
        # Caring loop infrastructure
        self.caring_thread = None
        self.caring_running = False
        self.caring_lock = threading.RLock()
        
        # Configuration defaults (will be overridden by apply_caring_config)
        self.caring_interval = 0.1
        self.caring_enabled_config = True
        self.caring_error_backoff = 1.0
        
        # Wake-up pipe for immediate updates
        self.caring_wake_r = None
        self.caring_wake_w = None
        
        # Error tracking
        self.caring_error_count = 0
        self.caring_last_error_time = 0
        
        # Values will be created in start_caring_loop() when network is ready
        self.caring_frequency = None
        self.caring_enabled = None
        self.caring_interval_val = None
        self.caring_error_count_val = None
        
        # Internal timing - not networked
        self.caring_last_update_time = 0.0
        self.caring_last_freq_update = 0.0

    def _create_caring_values(self):
        """Create values for caring loop monitoring and control."""
        self.caring_frequency = ValueDouble("caring_freq", "[Hz] caring loop frequency", initial=0.0)
        
        self.caring_enabled = ValueBool("caring_enabled", "caring loop enabled", writable=True, initial=True)
        
        self.caring_interval_val = ValueDouble("caring_interval", "[s] caring loop interval", 
                                             writable=True, initial=self.caring_interval)
        
        self.caring_error_count_val = ValueInteger("caring_errors", "caring loop error count", initial=0)
        
        # Internal timing - not networked
        self.caring_last_update_time = 0.0
        self.caring_last_freq_update = 0.0

    def start_caring_loop(self):
        """Start the caring loop thread."""
        if self.caring_thread and self.caring_thread.is_alive():
            logging.warning("Caring loop already running")
            return True
            
        if not self.caring_enabled_config:
            logging.info("Caring loop disabled by configuration")
            return True
            
        # Create values now that network is available
        if self.caring_frequency is None:
            self._create_caring_values()
            
        try:
            # Create wake-up pipe
            self.caring_wake_r, self.caring_wake_w = os.pipe()
            # Set non-blocking mode for reading end
            import fcntl
            fcntl.fcntl(self.caring_wake_r, fcntl.F_SETFL, os.O_NONBLOCK)
            
            # Start the thread
            self.caring_running = True
            self.caring_thread = threading.Thread(
                target=self._caring_loop_wrapper,
                name=f"{self.device_name}-Caring",
                daemon=True
            )
            self.caring_thread.start()
            
            logging.info(f"Caring loop started for {self.device_name} (interval: {self.caring_interval}s)")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start caring loop: {e}")
            self.caring_running = False
            return False

    def stop_caring_loop(self):
        """Stop the caring loop thread."""
        if not self.caring_running:
            return
            
        self.caring_running = False
        
        # Wake up the caring thread
        self._wake_caring_thread()
        
        # Wait for thread to finish
        if self.caring_thread and self.caring_thread.is_alive():
            self.caring_thread.join(timeout=2.0)
            if self.caring_thread.is_alive():
                logging.warning("Caring thread did not stop cleanly")
        
        # Close wake-up pipe
        if self.caring_wake_r is not None:
            os.close(self.caring_wake_r)
            self.caring_wake_r = None
        if self.caring_wake_w is not None:
            os.close(self.caring_wake_w)
            self.caring_wake_w = None
            
        logging.info(f"Caring loop stopped for {self.device_name}")

    def _wake_caring_thread(self):
        """Wake up the caring thread for immediate processing."""
        if self.caring_wake_w is not None:
            try:
                os.write(self.caring_wake_w, b'1')
            except OSError:
                pass  # Pipe might be full, that's OK

    def _caring_loop_wrapper(self):
        """Main caring loop with error handling and timing."""
        logging.info(f"__ Caring loop started for {self.device_name}")
        
        last_update_time = time.time()
        frequency_samples = []
        max_samples = 20
        
        logging.info(f"Caring loop running: {self.caring_running}")
        while self.caring_running:
            loop_start = time.time()
            
            logging.info("Caring loop!")
            try:
                # Check if caring is enabled
                if not self.caring_enabled.value:
                    self._wait_for_wake_or_timeout(1.0)  # Sleep longer when disabled
                    continue
                
                # Update interval from value if changed
                if hasattr(self, 'caring_interval_val'):
                    new_interval = self.caring_interval_val.value
                    if new_interval != self.caring_interval and new_interval > 0:
                        self.caring_interval = new_interval
                        logging.debug(f"Caring interval updated to {self.caring_interval}s")
                
                # Call device-specific caring logic
                with self.caring_lock:
                    self.device_care_update()
                
                # Update frequency calculation - but only publish occasionally
                current_time = time.time()
                if last_update_time > 0:
                    sample_time = current_time - last_update_time
                    frequency_samples.append(1.0 / sample_time if sample_time > 0 else 0)
                    
                    # Keep only recent samples
                    if len(frequency_samples) > max_samples:
                        frequency_samples.pop(0)
                    
                    # Update frequency value only periodically to reduce network load
                    if current_time - self.caring_last_freq_update >= self.caring_freq_update_interval:
                        if frequency_samples:
                            avg_freq = sum(frequency_samples) / len(frequency_samples)
                            self.caring_frequency.value = avg_freq
                            self.caring_last_freq_update = current_time
                
                last_update_time = current_time
                self.caring_last_update_time = current_time
                
                # Reset error count on successful update
                if self.caring_error_count > 0:
                    self.caring_error_count = 0
                    self.caring_error_count_val.value = 0
                
            except Exception as e:
                self.caring_error_count += 1
                self.caring_error_count_val.value = self.caring_error_count
                self.caring_last_error_time = time.time()
                
                logging.error(f"Error in caring loop for {self.device_name}: {e}")
                
                # Implement exponential backoff for errors
                error_sleep = min(self.caring_error_backoff * (2 ** min(self.caring_error_count - 1, 5)), 30.0)
                logging.debug(f"Caring error backoff: {error_sleep}s")
                self._wait_for_wake_or_timeout(error_sleep)
                continue
            
            # Calculate sleep time
            loop_duration = time.time() - loop_start
            sleep_time = max(0, self.caring_interval - loop_duration)
            
            # Wait for next update or wake signal
            if sleep_time > 0:
                self._wait_for_wake_or_timeout(sleep_time)
        
        logging.debug(f"Caring loop finished for {self.device_name}")

    def _wait_for_wake_or_timeout(self, timeout):
        """Wait for wake signal or timeout."""
        if self.caring_wake_r is None:
            time.sleep(timeout)
            return
            
        import select
        ready, _, _ = select.select([self.caring_wake_r], [], [], timeout)
        
        if ready:
            # Drain the wake pipe
            try:
                while True:
                    os.read(self.caring_wake_r, 1024)
            except OSError:
                pass  # No more data, expected

    def request_caring_update(self):
        """Request immediate caring update (non-blocking)."""
        self._wake_caring_thread()

    @abstractmethod
    def device_care_update(self):
        """
        Device-specific caring logic - override in subclass.
        
        This method is called regularly by the caring loop thread.
        It should implement device-specific monitoring logic such as:
        - Checking device limits
        - Updating position/status
        - Monitoring for error conditions
        - Performing periodic maintenance tasks
        
        The method should be thread-safe and handle exceptions gracefully.
        Exceptions will be caught and logged by the caring loop wrapper.
        """
        pass

    def on_value_changed_from_client(self, value, old_value, new_value):
        """Handle value changes from clients."""
        # Call parent handler if it exists
        if hasattr(super(), 'on_value_changed_from_client'):
            super().on_value_changed_from_client(value, old_value, new_value)
        
        # Handle caring-specific value changes
        if value == self.caring_enabled:
            if new_value and not old_value:
                logging.info("Caring loop enabled by client")
            elif not new_value and old_value:
                logging.info("Caring loop disabled by client")
        elif value == self.caring_interval_val:
            if new_value > 0 and new_value != old_value:
                logging.info(f"Caring interval changed to {new_value}s")
                # The caring loop will pick up the change automatically

    def get_caring_status(self) -> Dict[str, Any]:
        """Get current caring loop status for debugging."""
        return {
            'running': self.caring_running,
            'enabled': self.caring_enabled.value if hasattr(self, 'caring_enabled') else False,
            'interval': self.caring_interval,
            'frequency': self.caring_frequency.value if hasattr(self, 'caring_frequency') else 0,
            'error_count': self.caring_error_count,
            'last_error': self.caring_last_error_time,
            'last_update': self.caring_last_update_time
        }
