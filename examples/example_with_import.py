import math
from collections import Counter

def process_data(items, threshold):
    """Process a list of items with a threshold."""
    count = Counter(items)
    scaled = math.sqrt(threshold)
    result = sum(v for v in count.values() if v > scaled)
    return result
