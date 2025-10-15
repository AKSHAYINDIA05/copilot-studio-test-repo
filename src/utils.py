def clean_text(text: str, preserve_case: bool = False) -> str:
    """
    Clean and normalize text data
    
    Args:
        text: Input text to clean
        preserve_case: If True, preserve original capitalization
        
    Returns:
        Cleaned text string
    """
    if not text:
        return ""
    
    # Replace newlines with spaces
    cleaned = re.sub(r'\n+', ' ', text)
    
    # Replace multiple spaces with single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Strip leading/trailing whitespace
    cleaned = cleaned.strip()
    
    # Convert to lowercase only if not preserving case
    if not preserve_case:
        cleaned = cleaned.lower()
    
    return cleaned
