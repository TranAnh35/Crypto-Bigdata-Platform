# src/collectors/base_collector.py
from abc import ABC, abstractmethod

class Collector(ABC):
    """Abstract Base Class for all data collectors."""
    
    @abstractmethod
    def collect(self):
        """
        The main method to run the collection process.
        This must be implemented by subclasses.
        """
        pass