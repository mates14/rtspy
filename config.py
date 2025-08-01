# device_config.py - Simplified, argparse-like configuration system

import os
import configparser
import json
import logging
import argparse
from typing import Dict, Any, Optional, List, Union
from pathlib import Path


class ConfigArgument:
    """Represents a configuration argument that can come from multiple sources."""
    
    def __init__(self, *names, default=None, type=None, help=None, 
                 choices=None, action=None, section=None, **kwargs):
        """
        Define a configuration argument (similar to argparse.add_argument).
        
        Args:
            *names: Argument names (e.g., '--gcn-client-id', '-i')
            default: Default value
            type: Type conversion function
            help: Help text
            choices: Valid choices
            action: Action (store_true, store_false, etc.)
            section: Config file section (defaults to 'device')
            **kwargs: Additional argparse arguments
        """
        self.names = names
        self.default = default
        self.type = type
        self.help = help
        self.choices = choices
        self.action = action
        self.section = section or 'device'
        self.argparse_kwargs = kwargs
        
        # Determine config key from argument name
        self.config_key = self._get_config_key()
        
        # Determine environment variable name
        self.env_var = self._get_env_var()
    
    def _get_config_key(self) -> str:
        """Get configuration key from argument name."""
        # Use the longest name, remove dashes, convert to underscore
        longest_name = max(self.names, key=len)
        return longest_name.lstrip('-').replace('-', '_')
    
    def _get_env_var(self) -> str:
        """Get environment variable name."""
        return f"RTS2_{self.section.upper()}_{self.config_key.upper()}"
    
    def add_to_parser(self, parser: argparse.ArgumentParser):
        """Add this argument to an argparse parser."""
        kwargs = {
            'default': self.default,
            'help': self.help
        }
        
        if self.type is not None:
            kwargs['type'] = self.type
        if self.choices is not None:
            kwargs['choices'] = self.choices
        if self.action is not None:
            kwargs['action'] = self.action
        
        # Add any additional argparse kwargs
        kwargs.update(self.argparse_kwargs)
        
        parser.add_argument(*self.names, **kwargs)


class DeviceConfigRegistry:
    """Registry that manages all configuration arguments for a device."""
    
    def __init__(self):
        self.arguments = []
        self._standard_args_added = False
    
    def add_argument(self, *names, **kwargs) -> 'ConfigArgument':
        """
        Add a configuration argument (argparse-style interface).
        
        Usage:
            config.add_argument('--gcn-client-id', help='GCN client ID')
            config.add_argument('--port', type=int, default=0, section='network')
        """
        arg = ConfigArgument(*names, **kwargs)
        self.arguments.append(arg)
        return arg
    
    def add_standard_arguments(self):
        """Add standard RTS2 device arguments."""
        if self._standard_args_added:
            return
        
        # Device arguments
        self.add_argument('-d', '--device', help='Device name', section='device')
        self.add_argument('--simulation', action='store_true', 
                         help='Run in simulation mode', section='device')
        self.add_argument('--disable-device', action='store_true',
                         help='Start device in disabled state', section='device')
        
        # Network arguments
        self.add_argument('-P', '--port', type=int, default=0,
                         help='TCP/IP port for RTS2 communication', section='network')
        self.add_argument('-c', '--server', default='localhost',
                         help='Centrald hostname', section='network')
        self.add_argument('-p', '--server-port', type=int, default=617,
                         help='Centrald port', section='network')
        self.add_argument('--connection-timeout', type=float, default=300.0,
                         help='Connection timeout in seconds', section='network')
        
        # Logging arguments
        self.add_argument('-v', '--verbose', action='store_true',
                         help='Enable verbose logging', section='logging')
        self.add_argument('--debug', action='store_true',
                         help='Enable debug logging', section='logging')
        self.add_argument('--log-file', help='Log to file', section='logging')
        
        # Configuration arguments
        self.add_argument('--config', help='Configuration file path', section='meta')
        self.add_argument('--no-user-config', action='store_true',
                         help='Skip user config file', section='meta')
        self.add_argument('--no-system-config', action='store_true',
                         help='Skip system config file', section='meta')
        self.add_argument('--show-config', action='store_true',
                         help='Show resolved configuration and exit', section='meta')
        
        self._standard_args_added = True
    
    def register_with_parser(self, parser: argparse.ArgumentParser):
        """Register all arguments with an argparse parser."""
        for arg in self.arguments:
            arg.add_to_parser(parser)
    
    def resolve_configuration(self, args: argparse.Namespace) -> Dict[str, Any]:
        """
        Resolve configuration from all sources with proper priority.
        
        Returns a flat dictionary with all configuration values.
        """
        config = {}
        
        # Priority order (higher number = higher priority)
        sources = [
            (100, 'defaults', self._get_defaults()),
            (200, 'system_config', self._load_system_config()),
            (300, 'user_config', self._load_user_config(args)),
            (400, 'explicit_config', self._load_explicit_config(args)),
            (500, 'environment', self._load_environment()),
            (600, 'command_line', self._extract_from_args(args))
        ]
        
        # Apply sources in priority order
        for priority, source_name, source_data in sources:
            if source_data:
                config.update(source_data)
                logging.debug(f"Applied {source_name} configuration")
        
        return config
    
    def _get_defaults(self) -> Dict[str, Any]:
        """Get default values from argument definitions."""
        defaults = {}
        for arg in self.arguments:
            if arg.default is not None:
                defaults[arg.config_key] = arg.default
        return defaults
    
    def _load_system_config(self) -> Dict[str, Any]:
        """Load system configuration file."""
        system_paths = [
            '/etc/rts2/rts2.conf',
            '/usr/local/etc/rts2/rts2.conf'
        ]
        
        for path in system_paths:
            if os.path.exists(path):
                return self._parse_config_file(path)
        return {}
    
    def _load_user_config(self, args) -> Dict[str, Any]:
        """Load user configuration file."""
        if getattr(args, 'no_user_config', False):
            return {}
        
        user_paths = [
            os.path.expanduser('~/.rts2/rts2.conf'),
            os.path.expanduser('~/.rts2.conf')
        ]
        
        for path in user_paths:
            if os.path.exists(path):
                return self._parse_config_file(path)
        return {}
    
    def _load_explicit_config(self, args) -> Dict[str, Any]:
        """Load explicitly specified config file."""
        if hasattr(args, 'config') and args.config:
            return self._parse_config_file(args.config)
        return {}
    
    def _parse_config_file(self, path: str) -> Dict[str, Any]:
        """Parse configuration file and flatten to single-level dict."""
        try:
            if path.endswith('.json'):
                with open(path) as f:
                    nested_config = json.load(f)
            else:
                config_parser = configparser.ConfigParser()
                config_parser.read(path)
                nested_config = {section: dict(config_parser[section]) 
                               for section in config_parser.sections()}
            
            # Flatten nested config to match our argument keys
            flat_config = {}
            for arg in self.arguments:
                section_config = nested_config.get(arg.section, {})
                if arg.config_key in section_config:
                    value = section_config[arg.config_key]
                    # Convert string values to appropriate types
                    flat_config[arg.config_key] = self._convert_value(value, arg)
            
            return flat_config
            
        except Exception as e:
            logging.error(f"Error parsing config file {path}: {e}")
            return {}
    
    def _load_environment(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        env_config = {}
        for arg in self.arguments:
            if arg.env_var in os.environ:
                value = os.environ[arg.env_var]
                env_config[arg.config_key] = self._convert_value(value, arg)
        return env_config
    
    def _extract_from_args(self, args: argparse.Namespace) -> Dict[str, Any]:
        """Extract configuration from parsed command line arguments."""
        cli_config = {}
        for arg in self.arguments:
            if hasattr(args, arg.config_key):
                value = getattr(args, arg.config_key)
                if value is not None:
                    cli_config[arg.config_key] = value
        return cli_config
    
    def _convert_value(self, value: str, arg: ConfigArgument) -> Any:
        """Convert string value to appropriate type."""
        if arg.action == 'store_true':
            return value.lower() in ('true', 'yes', 'on', '1')
        elif arg.action == 'store_false':
            return value.lower() not in ('true', 'yes', 'on', '1')
        elif arg.type:
            return arg.type(value)
        else:
            return value
    
    def format_config_summary(self, config: Dict[str, Any]) -> str:
        """Format configuration for display."""
        summary = ["Configuration Values:"]
        
        # Group by section for display
        sections = {}
        for arg in self.arguments:
            if arg.section not in sections:
                sections[arg.section] = []
            
            value = config.get(arg.config_key, arg.default)
            # Mask sensitive values
            if any(word in arg.config_key.lower() for word in ['secret', 'password', 'key']):
                display_value = '***HIDDEN***' if value else None
            else:
                display_value = value
            
            sections[arg.section].append(f"  {arg.config_key} = {display_value}")
        
        for section, items in sections.items():
            summary.append(f"\n[{section}]")
            summary.extend(items)
        
        return '\n'.join(summary)


class DeviceConfig:
    """
    Simple mixin that provides argparse-like configuration for devices.
    
    Usage:
        class MyDevice(Device, DeviceConfig):
            def setup_config(self, config):
                config.add_argument('--my-option', help='My option')
                config.add_argument('--my-port', type=int, default=8080, 
                                   section='network', help='My port')
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._config_registry = DeviceConfigRegistry()
        self._resolved_config = {}
    
    @classmethod
    def register_options(cls, parser: argparse.ArgumentParser):
        """Register all device options with parser."""
        # Create temporary instance to set up configuration
        temp_registry = DeviceConfigRegistry()
        temp_registry.add_standard_arguments()
        
        # Let device add its specific arguments
        if hasattr(cls, 'setup_config'):
            temp_instance = cls.__new__(cls)  # Create without calling __init__
            temp_instance._config_registry = temp_registry
            temp_instance.setup_config(temp_registry)
        
        # Register all arguments with parser
        temp_registry.register_with_parser(parser)
    
    @classmethod
    def process_args(cls, device, args: argparse.Namespace):
        """Process arguments and apply configuration to device."""
        # Set up configuration registry
        device._config_registry.add_standard_arguments()
        
        # Let device add its specific arguments
        if hasattr(device, 'setup_config'):
            device.setup_config(device._config_registry)
        
        # Resolve configuration from all sources
        config = device._config_registry.resolve_configuration(args)
        device._resolved_config = config
        
        # Show configuration if requested
        if config.get('show_config', False):
            print(device._config_registry.format_config_summary(config))
            exit(0)
        
        # Apply configuration to device
        device._apply_resolved_config(config)
    
    def _apply_resolved_config(self, config: Dict[str, Any]):
        """Apply resolved configuration to device attributes."""
        # Apply standard configuration
        if 'device' in config:
            if config.get('device'):
                self.device_name = config['device']
                if hasattr(self, 'network'):
                    self.network.device_name = config['device']
        
        if config.get('simulation'):
            self.simulation_mode = True
        
        if config.get('disable_device'):
            self._state |= self.NOT_READY
        
        # Apply network configuration
        if hasattr(self, 'network'):
            if config.get('server'):
                self.network.centrald_host = config['server']
            if config.get('server_port'):
                self.network.centrald_port = config['server_port']
            if config.get('port'):
                self.network.port = config['port']
            if config.get('connection_timeout'):
                self.network.connection_timeout = config['connection_timeout']
        
        # Apply logging configuration
        if config.get('verbose'):
            logging.getLogger().setLevel(logging.INFO)
        if config.get('debug'):
            logging.getLogger().setLevel(logging.DEBUG)
        
        # Apply device-specific configuration
        self.apply_config(config)
    
    def apply_config(self, config: Dict[str, Any]):
        """
        Apply device-specific configuration.
        
        Override this method to handle device-specific configuration.
        All configuration values are available as a flat dictionary.
        """
        pass
    
    def get_config_value(self, key: str, default=None):
        """Get a configuration value."""
        return self._resolved_config.get(key, default)
    
    def get_config_summary(self) -> str:
        """Get configuration summary for debugging."""
        return self._config_registry.format_config_summary(self._resolved_config)


