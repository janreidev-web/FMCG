"""
Simple Sequential ID Generation Module for FMCG Analytics
Provides simple sequential ID generation functions with 6-digit padding standard
"""

# Global ID generation state to ensure uniqueness across all runs
ID_GENERATOR_STATE = {
    'sequence_counters': {}
}

def generate_unique_id(entity_type: str) -> int:
    """
    Generate simple sequential IDs
    
    Args:
        entity_type: Type of entity (e.g., 'employee', 'product', 'retailer')
    
    Returns:
        Sequential integer ID
    """
    # Get or initialize sequence counter for this entity type
    if entity_type not in ID_GENERATOR_STATE['sequence_counters']:
        ID_GENERATOR_STATE['sequence_counters'][entity_type] = 0
    
    # Increment sequence counter
    ID_GENERATOR_STATE['sequence_counters'][entity_type] += 1
    return ID_GENERATOR_STATE['sequence_counters'][entity_type]

def generate_readable_id(prefix: str, entity_type: str, padding: int = 6) -> str:
    """
    Generate readable sequential IDs with prefix
    
    Args:
        prefix: Prefix for the ID (e.g., 'EMP', 'P', 'R')
        entity_type: Type of entity for sequence tracking
        padding: Number of digits for zero-padding
    
    Returns:
        Formatted readable ID string
    """
    unique_num = generate_unique_id(entity_type)
    return f"{prefix}{unique_num:0{padding}d}"

def generate_unique_sale_key() -> int:
    """
    Generate simple sequential sale key
    
    Returns:
        Sequential sale key as integer
    """
    return generate_unique_id("sale")

def reset_id_counters(entity_type: str = None) -> None:
    """
    Reset ID generation counters. Use with caution!
    
    Args:
        entity_type: Specific entity type to reset, or None to reset all
    """
    if entity_type:
        ID_GENERATOR_STATE['sequence_counters'].pop(entity_type, None)
    else:
        ID_GENERATOR_STATE['sequence_counters'].clear()

def get_id_generation_stats() -> dict:
    """
    Get statistics about ID generation state
    
    Returns:
        Dictionary with generation statistics
    """
    return {
        'entity_counters': ID_GENERATOR_STATE['sequence_counters'].copy()
    }
