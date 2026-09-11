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
        # An explicit dest wins - otherwise adding a longer alias (say the
        # C++ spelling --local-port beside --port) would silently rename the
        # configuration key and orphan every reader of the old one.
        dest = self.argparse_kwargs.get('dest')
        if dest:
            return dest
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
        
        # Network arguments. --local-port is the C++ spelling of --port;
        # both are accepted so a line in /etc/rts2/devices written for a C++
        # driver works unchanged against a Python one.
        self.add_argument('-P', '--port', '--local-port', type=int, default=0,
                         dest='port',
                         help='TCP/IP port for RTS2 communication', section='network')
        self.add_argument('-c', '--server', default='localhost',
                         help='Centrald hostname, optionally as host:port',
                         section='network')
        self.add_argument('-p', '--server-port', type=int, default=617,
                         help='Centrald port', section='network')
        self.add_argument('--connection-timeout', type=float, default=300.0,
                         help='Connection timeout in seconds', section='network')

        # Daemon arguments - see rtspy/core/daemon.py and
        # docs/daemonising-rtspy.md for what each one is obliged to do.
        self.add_argument('-i', '--interactive', action='store_true',
                         dest='interactive',
                         help='run in interactive mode, do not fork to background',
                         section='daemon')
        self.add_argument('--lock-prefix', default=None,
                         help='prefix for lock file (default /var/run/rts2_)',
                         section='daemon')
        self.add_argument('--run-as', default=None,
                         help="run under specified user (and group, if provided after '.')",
                         section='daemon')
        self.add_argument('--daemonize-timeout', type=int, default=120,
                         help='seconds to wait for the daemon to finish initialising '
                              'before backgrounding it anyway (0 = wait forever)',
                         section='daemon')
        
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

        self._add_compat_arguments()

        self._standard_args_added = True

    # Options a C++ RTS2 daemon understands that rtspy has no equivalent for.
    # They are accepted rather than rejected so that a device line written for
    # the C++ driver never stops a Python one from starting - but anything
    # actually set is reported at startup, so a silently ignored option can be
    # found rather than wondered about.
    COMPAT_ARGUMENTS = [
        ('--autorestart', 1, 'seconds to wait for restart of crashed daemon'),
        ('--modefile', 1, 'file holding device modes'),
        ('--valuefile', 1, 'file with values which should be created on the device'),
        ('--autosave', 1, 'autosave file'),
        ('--defaults', 1, 'file with default values'),
        ('--localhost', 1, 'hostname, if different from gethostname()'),
        ('--noauth', 0, 'allow unauthorized connections'),
        ('--notcheck', 0, 'ignore if some recommended values are not set'),
    ]

    def _add_compat_arguments(self):
        """Accept the C++-only options without acting on them."""
        for name, takes_value, help_text in self.COMPAT_ARGUMENTS:
            if takes_value:
                self.add_argument(name, default=None,
                                  help='%s (accepted, not implemented)' % help_text,
                                  section='compat')
            else:
                self.add_argument(name, action='store_true',
                                  help='%s (accepted, not implemented)' % help_text,
                                  section='compat')

    def unimplemented_options(self, config: Dict[str, Any]) -> List[str]:
        """Which accepted-but-inert options the caller actually asked for."""
        used = []
        for name, _takes_value, _help in self.COMPAT_ARGUMENTS:
            key = name.lstrip('-').replace('-', '_')
            if config.get(key):
                used.append(name)
        return used
    
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

        self._normalise(config)

        return config

    @staticmethod
    def _normalise(config: Dict[str, Any]):
        """
        Fix up values whose C++ spelling carries more than one field.

        C++ takes the centrald address as a single --server host:port, rtspy
        as a hostname plus a separate --server-port. Accepting the combined
        form matters more than it looks: without this, --server sulafat:617
        is taken as a *hostname* of "sulafat:617" and the daemon simply fails
        to resolve it, with nothing in the log pointing at the real cause.
        The port given inside --server wins over --server-port.
        """
        server = config.get('server')
        if isinstance(server, str) and ':' in server:
            host, _, port = server.rpartition(':')
            try:
                config['server_port'] = int(port)
                config['server'] = host
            except ValueError:
                # not a port - leave it alone and let resolution complain
                pass
    
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
    def build_registry(cls) -> 'DeviceConfigRegistry':
        """
        Build this class's full argument registry without an instance.

        Deliberately free of any device: the daemon has to know its device
        name, lock prefix and -i flag *before* it forks, and it has to fork
        before it creates the device, because forking a process that has
        already started threads hands the child locked mutexes with no
        owners. setup_config() only ever touches the registry, so calling it
        on an uninitialised instance is safe.
        """
        registry = DeviceConfigRegistry()
        registry.add_standard_arguments()

        if hasattr(cls, 'setup_config'):
            temp_instance = cls.__new__(cls)  # Create without calling __init__
            temp_instance._config_registry = registry
            temp_instance.setup_config(registry)

        return registry

    @classmethod
    def register_options(cls, parser: argparse.ArgumentParser):
        """Register all device options with parser."""
        cls.build_registry().register_with_parser(parser)

    @classmethod
    def resolve_config(cls, args: argparse.Namespace):
        """
        Resolve the whole configuration with no device in existence.

        Returns (registry, config).
        """
        registry = cls.build_registry()
        return registry, registry.resolve_configuration(args)

    @classmethod
    def process_args(cls, device, args: argparse.Namespace,
                     registry: 'DeviceConfigRegistry' = None,
                     config: Dict[str, Any] = None):
        """
        Apply configuration to a device.

        A registry and config already resolved by resolve_config() can be
        passed in, so a daemon that had to resolve early (to know what to
        lock) does not parse everything a second time.
        """
        if registry is None or config is None:
            # Set up configuration registry
            device._config_registry.add_standard_arguments()

            # Let device add its specific arguments
            if hasattr(device, 'setup_config'):
                device.setup_config(device._config_registry)

            # Resolve configuration from all sources
            config = device._config_registry.resolve_configuration(args)
        else:
            device._config_registry = registry

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


