"""
Module for scanning and auto-loading modules.
Useful for triggering side-effects like:
- Handler registration (via __init_subclass__ or decorators)
- Event registration
- Persistence registration
- SQLAlchemy model discovery
"""
import pkgutil
import importlib
import logging
from typing import List, Union
from types import ModuleType

logger = logging.getLogger(__name__)

def scan_packages(packages: List[Union[str, ModuleType]]) -> None:
    """
    Recursively scan and import all modules in the given packages.
    
    Args:
        packages: List of package names (str) or module objects to scan.
    """
    for package in packages:
        if isinstance(package, str):
            try:
                package_module = importlib.import_module(package)
            except ImportError as e:
                logger.error(f"Failed to import package '{package}': {e}")
                continue
        else:
            package_module = package
            
        if not hasattr(package_module, "__path__"):
             # It's a module, not a package, nothing to scan inside
             continue
             
        # Scan submodules
        for module_info in pkgutil.walk_packages(package_module.__path__, package_module.__name__ + "."):
            try:
                importlib.import_module(module_info.name)
                logger.debug(f"Scanned module: {module_info.name}")
            except ImportError as e:
                logger.error(f"Failed to import module '{module_info.name}': {e}")
