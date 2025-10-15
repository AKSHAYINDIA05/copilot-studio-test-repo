@pytest.mark.parametrize("input_text,expected", [
    ('  Hello World  ', 'hello world'),  # Updated expectation
    ('Text\nWith\nNewlines', 'text with newlines'),  # Updated expectation  
    ('Multiple   Spaces', 'multiple spaces'),  # Updated expectation
    ('UPPERCASE', 'uppercase'),
    ('MiXeD cAsE', 'mixed case'),
    ('', ''),
])
def test_clean_text(self, input_text, expected):
    """Test text cleaning functionality"""
    result = clean_text(input_text)
    assert result == expected

def test_clean_text_preserve_case(self):
    """Test text cleaning with case preservation"""
    result = clean_text('  Hello World  ', preserve_case=True)
    assert result == 'Hello World'
    
    result = clean_text('Text\nWith\nNewlines', preserve_case=True)
    assert result == 'Text With Newlines'
