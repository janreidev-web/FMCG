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

def generate_unique_wage_key(employee_id: str, effective_date, sequence: int) -> str:
    """
    Generate unique wage key for employee wage records
    
    Args:
        employee_id: Employee ID
        effective_date: Effective date of the wage record
        sequence: Sequence number for this employee's wage history
    
    Returns:
        Unique wage key string
    """
    # Create a composite key that's unique per employee and date
    date_str = effective_date.strftime("%Y%m%d") if hasattr(effective_date, 'strftime') else str(effective_date)
    return f"WAGE_{employee_id}_{date_str}_{sequence:03d}"

def generate_unique_cost_key(cost_date, category: str, sequence: int) -> str:
    """
    Generate unique cost key for operating cost records
    
    Args:
        cost_date: Date of the cost record
        category: Cost category
        sequence: Sequence number for this date/category
    
    Returns:
        Unique cost key string
    """
    date_str = cost_date.strftime("%Y%m%d") if hasattr(cost_date, 'strftime') else str(cost_date)
    category_abbr = category[:3].upper() if len(category) > 3 else category.upper()
    return f"COST_{date_str}_{category_abbr}_{sequence:03d}"

def generate_unique_marketing_cost_key() -> int:
    """
    Generate simple sequential marketing cost key
    
    Returns:
        Sequential marketing cost key as integer
    """
    return generate_unique_id("marketing_cost")

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
