import re

# Parser configuration
PARSER_CONFIG = {
    'preserve_complex_structure': True
}

class ParsingError(Exception):
    """Exception raised for errors during parsing."""
    pass

def parse_script(script: str, meta: bool = False):
    """
    Parse a script written using set theory symbols into a structured Python representation.
    
    This implementation focuses on correctly handling quantifier expressions in any context,
    without relying on hardcoded patterns for specific scripts.
    
    Args:
        script (str): The script to parse
        
    Returns:
        When meta=False (default): parsed AST (unchanged behavior).
        When meta=True: dict with keys `ast` and `meta`.
    """
    # Use a completely generic approach without any pattern-specific optimizations
    # This ensures all expressions are handled consistently regardless of complexity
    import re
    
    # If no special pattern matched, continue with the regular parsing
    # Define all supported symbols by category
    ARITHMETIC_SYMBOLS = ['+', '-', '*', '/', '÷', '%']
    SET_OPERATION_SYMBOLS = ['∈', '∉', '⋂', '⋃', '⊖', '⊆', '⊂', '⊄', '∖', '⥹', '≪', '≫', '#']
    COMPARISON_SYMBOLS = ['≠', '=', '≈', '≥', '≤', '<', '>']
    UNARY_OPERATION_SYMBOLS = ['τ', '#', 'ℛ', '↲', '∄', '¬', '⊤', 'Φ', 'ℰ']
    ASSIGNMENT_SYMBOLS = ['≔', '⥻']
    DATA_STRUCTURE_SYMBOLS = ['ℒ', 'ℳ', '𝒯', '𝒮', '"', '@']
    LOGICAL_CONNECTIVE_SYMBOLS = ['⋀', '⋁']
    QUANTIFIER_SYMBOLS = ['∀', '∃']
    RANGE_SYMBOL = '‥'
    ELLIPSIS = '…'  # Ellipsis for unspecified trailing arguments in function calls
    
    # Combine all symbols for tokenization
    ALL_SYMBOLS = (ARITHMETIC_SYMBOLS + SET_OPERATION_SYMBOLS + COMPARISON_SYMBOLS + 
                  UNARY_OPERATION_SYMBOLS + ASSIGNMENT_SYMBOLS + DATA_STRUCTURE_SYMBOLS + 
                  LOGICAL_CONNECTIVE_SYMBOLS + QUANTIFIER_SYMBOLS + [RANGE_SYMBOL, ELLIPSIS, ':', '{', '}'])

    def tokenize(text):
        """Tokenize the input script into a list of tokens."""
        # Build a regex pattern that matches all symbols and tokens
        # IMPORTANT: Order matters! More specific patterns must come before general ones
        pattern_parts = []
        
        # Match numbers FIRST (including negative numbers and floats)
        # This must come before the minus operator to correctly parse negative numbers
        pattern_parts.extend([
            r'-?\d+(?:\.\d+)?',                       # Numbers (integers and floats, with optional negative sign)
            r'\'[^\']*\'',                            # Single-quoted strings
            r'\"[^\"]*\"',                            # Double-quoted strings
            r'[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*',  # Identifiers (with optional dots)
        ])
        
        # Then add all symbols
        for symbol in ALL_SYMBOLS:
            pattern_parts.append(re.escape(symbol))
        
        # Finally add brackets and separators
        pattern_parts.extend([
            r'\(', r'\)', r'\[', r'\]', r'\{', r'\}',  # Brackets
            r',', r':',                               # Separators
        ])
        
        pattern = '|'.join(pattern_parts)
        tokens = re.findall(pattern, text)
        return [token.strip() for token in tokens if token.strip()]

    def tokenize_with_spans(text):
        """Tokenize and keep character offsets for metadata generation."""
        pattern_parts = []
        pattern_parts.extend([
            r'-?\d+(?:\.\d+)?',
            r'\'[^\']*\'',
            r'\"[^\"]*\"',
            r'[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*',
        ])

        for symbol in ALL_SYMBOLS:
            pattern_parts.append(re.escape(symbol))

        pattern_parts.extend([
            r'\(', r'\)', r'\[', r'\]', r'\{', r'\}',
            r',', r':',
        ])

        pattern = '|'.join(pattern_parts)
        token_values = []
        token_spans = []
        for match in re.finditer(pattern, text):
            value = match.group(0)
            if value and value.strip():
                token_values.append(value.strip())
                token_spans.append((match.start(), match.end()))
        return token_values, token_spans

    def compute_start_end(token_spans, start_token_idx, end_token_idx):
        """Compute char start/end from token-index bounds (inclusive end token)."""
        if not token_spans:
            return 0, 0
        if start_token_idx < 0:
            start_token_idx = 0
        if end_token_idx < start_token_idx:
            end_token_idx = start_token_idx
        if start_token_idx >= len(token_spans):
            start_token_idx = len(token_spans) - 1
        if end_token_idx >= len(token_spans):
            end_token_idx = len(token_spans) - 1
        return token_spans[start_token_idx][0], token_spans[end_token_idx][1]

    def extract_text_slice(text, start, end):
        """Extract raw script substring using char offsets."""
        if start < 0:
            start = 0
        if end < start:
            end = start
        if end > len(text):
            end = len(text)
        return text[start:end]

    def infer_node_type(node):
        """Infer a stable metadata node type label from AST shape/value."""
        if isinstance(node, list):
            if any(isinstance(x, str) and x in LOGICAL_CONNECTIVE_SYMBOLS for x in node):
                return 'chain'
            return 'list'

        if isinstance(node, tuple) and len(node) >= 1:
            op = node[0]
            if op in ASSIGNMENT_SYMBOLS:
                return 'assignment'
            if op in LOGICAL_CONNECTIVE_SYMBOLS:
                return 'chain'
            if op in QUANTIFIER_SYMBOLS:
                return 'quantifier'
            if op in UNARY_OPERATION_SYMBOLS:
                return 'unary'
            if op in ARITHMETIC_SYMBOLS or op in SET_OPERATION_SYMBOLS or op in COMPARISON_SYMBOLS:
                return 'predicate'
            if op in DATA_STRUCTURE_SYMBOLS:
                return 'data_structure'
            return 'predicate'

        if isinstance(node, (int, float, bool)):
            return 'literal'
        if isinstance(node, str):
            return 'identifier'
        return 'node'

    def build_metadata_node(start, end, path, node_type, text, children=None):
        """Build one metadata node payload."""
        return {
            'start': start,
            'end': end,
            'path': list(path),
            'node_type': node_type,
            'text': text,
            'children': children or []
        }

    def build_metadata_tree(ast_node, script_text, token_spans, path, cursor):
        """
        Build metadata tree mirroring AST structure.

        The traversal is pre-order and consumes token spans in the same order to produce
        stable offsets for each node without changing AST semantics.
        """
        start_token_idx = cursor[0]
        children = []

        if isinstance(ast_node, tuple):
            for idx, child in enumerate(ast_node):
                children.append(
                    build_metadata_tree(
                        child,
                        script_text,
                        token_spans,
                        path + [idx],
                        cursor,
                    )
                )
        elif isinstance(ast_node, list):
            for idx, child in enumerate(ast_node):
                children.append(
                    build_metadata_tree(
                        child,
                        script_text,
                        token_spans,
                        path + [idx],
                        cursor,
                    )
                )
        else:
            # Leaf node consumes one token span when available.
            if cursor[0] < len(token_spans):
                cursor[0] += 1

        end_token_idx = max(cursor[0] - 1, start_token_idx)

        if token_spans:
            start, end = compute_start_end(token_spans, start_token_idx, end_token_idx)
        else:
            start, end = 0, len(script_text)

        node_type = infer_node_type(ast_node)
        text = extract_text_slice(script_text, start, end)
        return build_metadata_node(start, end, path, node_type, text, children)

    def parse_string_literal(token):
        """Parse a string literal into the required format."""
        content = token[1:-1]  # Remove outer quotes
        return ('"', [content])

    def parse_assignment(left, right, operator='≔'):
        """Parse an assignment expression with any supported assignment operator."""
        return (operator, [left, right])

    def parse_binary_operation(left, operator, right):
        """Parse a binary operation."""
        return (operator, [left, right])

    def parse_list(elements):
        """Convert a list of elements to the expected format."""
        return ('ℒ', elements)

    def parse_dict(elements):
        """Convert a dictionary to the expected format.
        
        Elements should be a list of [key, value] pairs.
        For example: [['_c', 'abc'], ['_d', 123]]
        """
        return ('ℳ', elements)
        
    def parse_set(elements):
        """Convert a set of elements to the expected format."""
        return ('𝒮', elements)

    def parse_tuple(elements):
        """Convert a tuple of elements to the expected format."""
        return ('𝒯', elements)

    def parse_element_access(container, index):
        """Parse element access (list/dict indexing)."""
        return ('@', [container, index])

    def parse_unary_operation(operator, operand):
        """Parse a unary operation."""
        # Special case for quantifiers - always format as (quantifier, [variable])
        if operator in QUANTIFIER_SYMBOLS:
            # Ensure operand is in a list
            if not isinstance(operand, list):
                operand = [operand]
            return (operator, operand)
        else:
            return (operator, [operand])

    def parse_function_call(function_name, arguments):
        """Parse a function call."""
        return (function_name, arguments)

    def parse_quantifier_iteration(quantifier, variables, iterable):
        """Parse a quantifier with iteration."""
        # Check if variables is a comma-separated list that should be a tuple
        if isinstance(variables, list) and ',' in variables:
            # Find the comma and extract the elements
            elements = []
            current = []
            for token in variables:
                if token == ',':
                    if current:
                        elements.append(''.join(current))
                        current = []
                else:
                    current.append(token)
            
            if current:
                elements.append(''.join(current))
                
            # Convert to a tuple format
            variables = ('𝒯', elements)
        
        # Use quantifier symbol directly without adding '∈'
        return (quantifier, [variables, iterable])

    def parse_range(start, end):
        """Parse a range expression [start‥end] and expand it."""
        try:
            start_val = int(start)
            end_val = int(end)
            if start_val <= end_val:
                return ('ℒ', list(range(start_val, end_val + 1)))
            else:
                return ('ℒ', list(range(start_val, end_val - 1, -1)))
        except ValueError:
            raise ParsingError(f"Range bounds must be integers: [{start}‥{end}]")

    def find_matching_parenthesis(tokens, start_idx):
        """Find the index of the matching closing parenthesis."""
        paren_count = 1
        for i in range(start_idx + 1, len(tokens)):
            if tokens[i] == '(':
                paren_count += 1
            elif tokens[i] == ')':
                paren_count -= 1
                if paren_count == 0:
                    return i
        return -1
        
    def find_top_level_binary_operator(tokens, operator_list):
        """Find the index of a binary operator at the top level of tokens."""
        paren_level = 0
        for i, token in enumerate(tokens):
            if token == '(':
                paren_level += 1
            elif token == ')':
                paren_level -= 1
            elif token in operator_list and paren_level == 0:
                return i
        return -1

    def parse_expression(tokens):
        """Parse a simple expression (not an expression group)."""
        if not tokens:
            return None
            
        # Handle single token expressions
        if len(tokens) == 1:
            token = tokens[0]
            if token == 'true':
                return True
            elif token == 'false':
                return False
            elif token == ELLIPSIS:
                # Preserve ellipsis as-is for function call arguments
                return ELLIPSIS
            elif token.replace('.', '', 1).isdigit() or (token.startswith('-') and token[1:].replace('.', '', 1).isdigit()):
                # Handle positive/negative integers and floats
                if '.' in token:
                    return float(token)
                else:
                    return int(token)
            elif token.startswith("'") and token.endswith("'"):
                # Single-quoted strings use the same format as double-quoted strings
                content = token[1:-1]  # Remove quotes
                return ('"', [content])
            elif token.startswith('"') and token.endswith('"'):
                # Use special format for double-quoted strings
                return parse_string_literal(token)
            else:
                return token
                
        # Check for operations at the top level first (outside any parentheses)
        # This ensures expressions like (_x + _y) * (_z - _w) parse correctly
        
        # Check for binary operations with set operations
        op_idx = find_top_level_binary_operator(tokens, SET_OPERATION_SYMBOLS)
        if op_idx > 0:
            left_tokens = tokens[:op_idx]
            right_tokens = tokens[op_idx+1:]
            
            left = parse_expression(left_tokens)
            right = parse_expression(right_tokens)
            
            return parse_binary_operation(left, tokens[op_idx], right)
        
        # Check for binary operations with arithmetic operators
        op_idx = find_top_level_binary_operator(tokens, ARITHMETIC_SYMBOLS)
        if op_idx > 0:
            left_tokens = tokens[:op_idx]
            right_tokens = tokens[op_idx+1:]
            
            left = parse_expression(left_tokens)
            right = parse_expression(right_tokens)
            
            return parse_binary_operation(left, tokens[op_idx], right)

        # Handle assignments at top level before tuple-literal detection.
        # This preserves shorthand tuple destructuring syntax like:
        # (_a, _b, _c) ≔ ([1, 2, 3], abc, 5)
        assign_idx = -1
        assign_op = None
        for op in ASSIGNMENT_SYMBOLS:
            idx = find_top_level_binary_operator(tokens, [op])
            if idx > 0 and (assign_idx == -1 or idx < assign_idx):
                assign_idx = idx
                assign_op = op

        if assign_idx > 0 and assign_op is not None:
            left_tokens = tokens[:assign_idx]
            right_tokens = tokens[assign_idx + 1:]
            left = parse_expression(left_tokens)
            right = parse_expression(right_tokens)
            return parse_assignment(left, right, assign_op)

        # Handle special case for nested parentheses: ((_x + _y) = 15)
        # BUT NOT for logical connectives - those are handled by the parenthesized expression handler
        if tokens[0] == '(' and tokens[-1] == ')' and tokens[1] == '(':
            # Find the matching closing parenthesis for the inner expression
            inner_close_idx = find_matching_parenthesis(tokens, 1)
            if inner_close_idx > 0 and inner_close_idx < len(tokens) - 2:
                # Check if the operator after the inner expression is a logical connective
                next_token = tokens[inner_close_idx + 1]
                # Skip this handler for logical connectives - they have their own handler
                if next_token not in LOGICAL_CONNECTIVE_SYMBOLS:
                    # Parse the inner expression
                    inner_expr = tokens[2:inner_close_idx]
                    inner_result = parse_expression(inner_expr)
                    
                    # Check if there's a binary operator after the inner closing parenthesis
                    if next_token in (ARITHMETIC_SYMBOLS + COMPARISON_SYMBOLS + SET_OPERATION_SYMBOLS):
                        # Parse the right side of the operation
                        right_expr = tokens[inner_close_idx + 2:-1]
                        right_result = parse_expression(right_expr)
                        
                        # For equality operations, use the special format (wrapped in list)
                        if next_token == '=':
                            return [('=', [inner_result, right_result])]
                        # For other operators (comparisons, arithmetic, set ops), return unwrapped
                        else:
                            return (next_token, [inner_result, right_result])
            
        # Handle tuple literals BEFORE generic parenthesized expressions
        # Check if this is a tuple: (a, b, c)
        if tokens[0] == '(' and tokens[-1] == ')' and ',' in tokens:
            # Check if this is a tuple rather than a parenthesized expression
            has_comma_at_top_level = False
            paren_depth = 0
            bracket_depth = 0
            brace_depth = 0
            for token in tokens[1:-1]:  # Skip outermost parentheses
                if token == '(':
                    paren_depth += 1
                elif token == ')':
                    paren_depth -= 1
                elif token == '[':
                    bracket_depth += 1
                elif token == ']':
                    bracket_depth -= 1
                elif token == '{':
                    brace_depth += 1
                elif token == '}':
                    brace_depth -= 1
                elif token == ',' and paren_depth == 0 and bracket_depth == 0 and brace_depth == 0:
                    has_comma_at_top_level = True
                    break
            
            if has_comma_at_top_level:
                # This is a tuple with comma-separated elements
                inner_tokens = tokens[1:-1]  # Remove outer parentheses
                
                # Split by commas at top level
                elements = []
                current = []
                paren_count = 0
                bracket_count = 0
                brace_count = 0
                
                for token in inner_tokens:
                    if token == '(':
                        paren_count += 1
                        current.append(token)
                    elif token == ')':
                        paren_count -= 1
                        current.append(token)
                    elif token == '[':
                        bracket_count += 1
                        current.append(token)
                    elif token == ']':
                        bracket_count -= 1
                        current.append(token)
                    elif token == '{':
                        brace_count += 1
                        current.append(token)
                    elif token == '}':
                        brace_count -= 1
                        current.append(token)
                    elif token == ',' and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                        # Process the element before the comma
                        if current:
                            elements.append(parse_expression(current))
                            current = []
                    else:
                        current.append(token)
                
                # Process the last element
                if current:
                    elements.append(parse_expression(current))
                
                return parse_tuple(elements)
        
        # Handle parenthesized expressions
        # Verify that the first and last parentheses actually match
        if tokens[0] == '(' and tokens[-1] == ')':
            matching_close = find_matching_parenthesis(tokens, 0)
            if matching_close == len(tokens) - 1:
                inner_tokens = tokens[1:-1]
                
                # Check for standalone quantifier expressions
                # Only treat as standalone if there are no more tokens after the quantifier
                if inner_tokens and inner_tokens[0] in QUANTIFIER_SYMBOLS:
                    if len(inner_tokens) > 2 and inner_tokens[1] == '(':
                        # Find matching closing parenthesis
                        close_idx = find_matching_parenthesis(inner_tokens, 1)
                        if close_idx > 0:
                            var_tokens = inner_tokens[2:close_idx]
                            variable = parse_expression(var_tokens)
                            
                            # Check if this is a quantifier iteration or a standalone quantifier
                            if close_idx + 1 < len(inner_tokens) and inner_tokens[close_idx + 1] == '∈':
                                # This is a quantifier iteration
                                iterable_tokens = inner_tokens[close_idx + 2:]
                                iterable = parse_expression(iterable_tokens)
                                return parse_quantifier_iteration(inner_tokens[0], variable, iterable)
                            elif close_idx + 1 >= len(inner_tokens):
                                # This is a standalone quantifier - no more tokens after it
                                return (inner_tokens[0], [variable])
                            # If there are more tokens, fall through to regular parsing
                
                # If not a special quantifier, parse the inner tokens
                # Check if inner tokens contain a top-level logical operator
                has_logical = False
                paren_count = 0
                for token in inner_tokens:
                    if token == '(':
                        paren_count += 1
                    elif token == ')':
                        paren_count -= 1
                    elif token in LOGICAL_CONNECTIVE_SYMBOLS and paren_count == 0:
                        has_logical = True
                        break
                
                if has_logical:
                    result = parse_expression_group(inner_tokens)
                else:
                    result = parse_expression(inner_tokens)
                return result
            else:
                # Outer parentheses don't match - check if there's a top-level logical operator
                # This handles cases like ((expr1) ⋀ (expr2)) where the first paren is not matched by the last
                inner_tokens = tokens[1:-1]
                has_logical = False
                paren_count = 0
                for token in inner_tokens:
                    if token == '(':
                        paren_count += 1
                    elif token == ')':
                        paren_count -= 1
                    elif token in LOGICAL_CONNECTIVE_SYMBOLS and paren_count == 0:
                        has_logical = True
                        break
                
                if has_logical:
                    # Parse as expression group with logical operators
                    result = parse_expression_group(inner_tokens)
                    return result
                # Otherwise fall through to other handlers
        
        # Handle assignments (≔ or ⥻)
        # Fallback path retained for compatibility with complex/non-standard forms.
        for op in ASSIGNMENT_SYMBOLS:
            if op in tokens:
                assign_idx = tokens.index(op)
                left_tokens = tokens[:assign_idx]
                right_tokens = tokens[assign_idx + 1:]

                left = parse_expression(left_tokens)
                right = parse_expression(right_tokens)

                return parse_assignment(left, right, op)
        
        # Handle dictionary literals {key: value, key2: value2}
        if tokens[0] == '{' and tokens[-1] == '}' and ':' in tokens:
            inner_tokens = tokens[1:-1]
            dict_entries = []
            
            # Parse the entries
            if inner_tokens:
                # Split by commas at the top level
                current_entry = []
                nesting_level = 0
                
                for token in inner_tokens:
                    if token in ['(', '[', '{']:
                        nesting_level += 1
                        current_entry.append(token)
                    elif token in [')', ']', '}']:
                        nesting_level -= 1
                        current_entry.append(token)
                    elif token == ',' and nesting_level == 0:
                        # End of entry
                        if current_entry and ':' in current_entry:
                            colon_idx = current_entry.index(':')
                            key_tokens = current_entry[:colon_idx]
                            value_tokens = current_entry[colon_idx+1:]
                            
                            if key_tokens and value_tokens:
                                key = parse_expression(key_tokens)
                                value = parse_expression(value_tokens)
                                dict_entries.append([key, value])
                        
                        current_entry = []  # Reset for next entry
                    else:
                        current_entry.append(token)
                
                # Handle last entry if any
                if current_entry and ':' in current_entry:
                    colon_idx = current_entry.index(':')
                    key_tokens = current_entry[:colon_idx]
                    value_tokens = current_entry[colon_idx+1:]
                    
                    if key_tokens and value_tokens:
                        key = parse_expression(key_tokens)
                        value = parse_expression(value_tokens)
                        dict_entries.append([key, value])
            
            return parse_dict(dict_entries)
        
        # Handle range expressions
        if tokens[0] == '[' and RANGE_SYMBOL in tokens and tokens[-1] == ']':
            range_idx = tokens.index(RANGE_SYMBOL)
            if range_idx > 0 and range_idx < len(tokens) - 1:
                start = tokens[range_idx - 1]
                end = tokens[range_idx + 1]
                return parse_range(start, end)
        
        # Handle binary operations
        # Check for binary operations with comparison operators  
        op_idx = find_top_level_binary_operator(tokens, COMPARISON_SYMBOLS)
        if op_idx > 0:
            left_tokens = tokens[:op_idx]
            right_tokens = tokens[op_idx + 1:]
            
            left = parse_expression(left_tokens)
            right = parse_expression(right_tokens)
            
            return parse_binary_operation(left, tokens[op_idx], right)
        
        # Handle list literals
        if tokens[0] == '[' and tokens[-1] == ']':
            if len(tokens) == 2:  # Empty list
                return parse_list([])
                
            # Parse list elements
            elements = []
            current = []
            paren_count = 0
            bracket_count = 0
            brace_count = 0
            
            for i in range(1, len(tokens) - 1):  # Skip brackets
                token = tokens[i]
                
                # Track nesting level
                if token == '(':
                    paren_count += 1
                elif token == ')':
                    paren_count -= 1
                elif token == '[':
                    bracket_count += 1
                elif token == ']':
                    bracket_count -= 1
                elif token == '{':
                    brace_count += 1
                elif token == '}':
                    brace_count -= 1
                
                # Process comma-separated elements
                if token == ',' and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                    if current:
                        elements.append(parse_expression(current))
                        current = []
                    continue
                
                current.append(token)
            
            if current:
                elements.append(parse_expression(current))
                
            return parse_list(elements)
        
        # Handle dictionary or set literals (both use curly braces {})
        if tokens[0] == '{' and tokens[-1] == '}':
            # Explicit empty container forms
            # {}   -> empty dict/map
            # {ℳ} -> empty dict/map
            # {𝒮} -> empty set
            if len(tokens) == 2:
                return parse_dict([])
            if len(tokens) == 3 and tokens[1] == 'ℳ':
                return parse_dict([])
            if len(tokens) == 3 and tokens[1] == '𝒮':
                return parse_set([])

            # A top-level ':' means dict; otherwise set
            is_dictionary = False
            paren_count = 0
            bracket_count = 0
            brace_count = 0

            for token in tokens[1:-1]:
                if token == '(':
                    paren_count += 1
                elif token == ')':
                    paren_count -= 1
                elif token == '[':
                    bracket_count += 1
                elif token == ']':
                    bracket_count -= 1
                elif token == '{':
                    brace_count += 1
                elif token == '}':
                    brace_count -= 1
                elif token == ':' and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                    is_dictionary = True
                    break

            if is_dictionary:
                pairs = []
                i = 1

                while i < len(tokens) - 1:
                    key_start = i
                    colon_idx = -1

                    paren_count = 0
                    bracket_count = 0
                    brace_count = 0

                    for j in range(i, len(tokens) - 1):
                        token = tokens[j]

                        if token == '(':
                            paren_count += 1
                        elif token == ')':
                            paren_count -= 1
                        elif token == '[':
                            bracket_count += 1
                        elif token == ']':
                            bracket_count -= 1
                        elif token == '{':
                            brace_count += 1
                        elif token == '}':
                            brace_count -= 1
                        elif token == ':' and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                            colon_idx = j
                            break

                    if colon_idx == -1:
                        break

                    key_tokens = tokens[key_start:colon_idx]
                    value_start = colon_idx + 1
                    value_end = -1

                    paren_count = 0
                    bracket_count = 0
                    brace_count = 0

                    for j in range(value_start, len(tokens) - 1):
                        token = tokens[j]

                        if token == '(':
                            paren_count += 1
                        elif token == ')':
                            paren_count -= 1
                        elif token == '[':
                            bracket_count += 1
                        elif token == ']':
                            bracket_count -= 1
                        elif token == '{':
                            brace_count += 1
                        elif token == '}':
                            brace_count -= 1
                        elif token == ',' and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                            value_end = j
                            break

                    if value_end == -1:
                        value_tokens = tokens[value_start:len(tokens) - 1]
                        i = len(tokens) - 1
                    else:
                        value_tokens = tokens[value_start:value_end]
                        i = value_end + 1

                    key = parse_expression(key_tokens)
                    value = parse_expression(value_tokens)

                    if isinstance(key, tuple) and key[0] == '"':
                        key = key[1][0]

                    pairs.append([key, value])

                return parse_dict(pairs)

            elements = []
            current = []
            paren_count = 0
            bracket_count = 0
            brace_count = 0

            for i in range(1, len(tokens) - 1):
                token = tokens[i]

                if token == '(':
                    paren_count += 1
                    current.append(token)
                elif token == ')':
                    paren_count -= 1
                    current.append(token)
                elif token == '[':
                    bracket_count += 1
                    current.append(token)
                elif token == ']':
                    bracket_count -= 1
                    current.append(token)
                elif token == '{':
                    brace_count += 1
                    current.append(token)
                elif token == '}':
                    brace_count -= 1
                    current.append(token)
                elif token == ',' and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                    if current:
                        elements.append(parse_expression(current))
                        current = []
                else:
                    current.append(token)

            if current:
                elements.append(parse_expression(current))

            return parse_set(elements)
        
        # Tolerate accidental empty-call suffix on indexed access, e.g. age[Yeung]().
        # Treat it as plain age[Yeung] for user-facing Program mode robustness.
        if len(tokens) >= 5 and tokens[-2] == '(' and tokens[-1] == ')':
            inner_tokens = tokens[:-2]
            if len(inner_tokens) >= 3 and inner_tokens[1] == '[' and inner_tokens[-1] == ']':
                container = parse_expression([inner_tokens[0]])
                index_tokens = inner_tokens[2:-1]
                index = parse_expression(index_tokens)
                return parse_element_access(container, index)

        # Handle function calls
        # Check if first token is an identifier (starts with letter or underscore)
        if (len(tokens) >= 3 and 
            isinstance(tokens[0], str) and 
            len(tokens[0]) > 0 and 
            (tokens[0][0].isalpha() or tokens[0][0] == '_') and
            tokens[1] == '(' and tokens[-1] == ')'):
            func_name = tokens[0]
            arg_tokens = tokens[2:-1]  # Skip function name and parentheses
            
            if not arg_tokens:  # Empty arguments
                return parse_function_call(func_name, [])
                
            # Parse comma-separated arguments
            args = []
            current = []
            paren_count = 0
            bracket_count = 0
            brace_count = 0
            
            for token in arg_tokens:
                if token == '(':
                    paren_count += 1
                elif token == ')':
                    paren_count -= 1
                elif token == '[':
                    bracket_count += 1
                elif token == ']':
                    bracket_count -= 1
                elif token == '{':
                    brace_count += 1
                elif token == '}':
                    brace_count -= 1
                elif token == ',' and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                    if current:
                        args.append(parse_expression(current))
                        current = []
                    continue
                
                current.append(token)
            
            if current:
                args.append(parse_expression(current))
                
            return parse_function_call(func_name, args)
        
        # Handle unary operations and quantifiers
        if tokens[0] in UNARY_OPERATION_SYMBOLS or tokens[0] in QUANTIFIER_SYMBOLS:
            # Special handling for quantifiers: they only consume the immediate variable spec
            if tokens[0] in QUANTIFIER_SYMBOLS:
                # Quantifier should be followed by parenthesized variable(s)
                if len(tokens) >= 3 and tokens[1] == '(':
                    close_idx = find_matching_parenthesis(tokens, 1)
                    if close_idx > 0:
                        var_tokens = tokens[2:close_idx]
                        variable = parse_expression(var_tokens)
                        
                        # Check if there's more after the quantifier
                        if close_idx + 1 < len(tokens):
                            # There are more tokens - this quantifier is part of a larger expression
                            # We need to handle this at a higher level
                            # Don't consume it here, let it fall through
                            pass
                        else:
                            # Standalone quantifier
                            if not isinstance(variable, list):
                                variable = [variable]
                            return (tokens[0], variable)
            
            # For non-quantifier unary operations, consume all remaining tokens
            if tokens[0] in UNARY_OPERATION_SYMBOLS:
                operand_tokens = tokens[1:]
                
                # Special handling for parenthesized operands. Only unwrap when the opening
                # parenthesis actually matches the relevant closing parenthesis; this avoids
                # swallowing caller-owned trailing ')' tokens in nested logical expressions.
                if operand_tokens and operand_tokens[0] == '(':
                    matching_close = find_matching_parenthesis(operand_tokens, 0)
                    if matching_close > 0:
                        trailing_tokens = operand_tokens[matching_close + 1:]
                        if matching_close == len(operand_tokens) - 1 or all(token == ')' for token in trailing_tokens):
                            inner_tokens = operand_tokens[1:matching_close]
                            operand = parse_expression_group(inner_tokens)
                            return (tokens[0], [operand])

                # Check if there's a logical operator at top level in the remaining operand tokens.
                # This handles unary operators whose operand is not a single matched parenthesized
                # group.
                else:
                    # Check if there's a logical operator at top level
                    has_logical_at_top = False
                    paren_count = 0
                    bracket_count = 0
                    brace_count = 0
                    for token in operand_tokens:
                        if token == '(':
                            paren_count += 1
                        elif token == ')':
                            paren_count -= 1
                        elif token == '[':
                            bracket_count += 1
                        elif token == ']':
                            bracket_count -= 1
                        elif token == '{':
                            brace_count += 1
                        elif token == '}':
                            brace_count -= 1
                        elif token in LOGICAL_CONNECTIVE_SYMBOLS and paren_count == 0 and bracket_count == 0 and brace_count == 0:
                            has_logical_at_top = True
                            break
                    
                    if has_logical_at_top:
                        operand = parse_expression_group(operand_tokens)
                    else:
                        operand = parse_expression(operand_tokens)
                    return (tokens[0], [operand])

                operand = parse_expression(operand_tokens)
                return (tokens[0], [operand])
            
        # Handle element access with parentheses (for dictionaries)
        if len(tokens) >= 3 and tokens[1] == '(' and tokens[-1] == ')':
            container = parse_expression([tokens[0]])
            key_tokens = tokens[2:-1]
            key = parse_expression(key_tokens)
            return parse_element_access(container, key)
        
        # Handle element access with square brackets (for lists)
        if len(tokens) >= 3 and tokens[1] == '[' and tokens[-1] == ']':
            container = parse_expression([tokens[0]])
            index_tokens = tokens[2:-1]
            index = parse_expression(index_tokens)
            return parse_element_access(container, index)
        
        
        # Default case: return tokens as is
        return tokens

    def find_logical_operator_index(tokens):
        """Find the index of a logical operator at the top level of nesting."""
        paren_count = 0
        for i, token in enumerate(tokens):
            if token == '(':
                paren_count += 1
            elif token == ')':
                paren_count -= 1
            elif token in LOGICAL_CONNECTIVE_SYMBOLS and paren_count == 0:
                return i
        return -1
        
    def parse_unary_operator_after_logical(left_expr, tokens):
        """
        Handle cases where a unary operator follows a logical connective.
        Example: (∃(_x) ⋀ ↲(_x))
        """
        # Check if the tokens start with a unary operator followed by parenthesis
        if len(tokens) >= 2 and tokens[0] in UNARY_OPERATION_SYMBOLS and tokens[1] == '(':
            # Find matching closing parenthesis
            close_idx = find_matching_parenthesis(tokens, 1)
            if close_idx > 0:
                # Get the operand tokens
                operand_tokens = tokens[2:close_idx]
                operand = parse_expression(operand_tokens)
                unary_op = (tokens[0], [operand])
                
                # If more tokens exist after the closing parenthesis, they might be part of 
                # another logical connective
                if close_idx + 1 < len(tokens):
                    more_tokens = tokens[close_idx + 1:]
                    if more_tokens[0] in LOGICAL_CONNECTIVE_SYMBOLS:
                        # This is a logical connective followed by another expression
                        logical_op = more_tokens[0]
                        right_tokens = more_tokens[1:]
                        right_expr = parse_expression_group(right_tokens)
                        return [left_expr, logical_op, [unary_op, logical_op, right_expr]]
                    else:
                        # This is just additional tokens, for now we'll append them
                        return [left_expr, '⋀', unary_op]
                else:
                    # Just the unary operation follows the logical connective
                    return [left_expr, '⋀', unary_op]
                    
        # Default case: return a simple logical connection
        right_expr = parse_expression_group(tokens)
        return [left_expr, '⋀', right_expr]

    def parse_expression_group(tokens):
        """Parse an expression or expression group that may contain logical connectives."""
        # Check for logical connectives at the top level
        op_idx = find_logical_operator_index(tokens)
        
        if op_idx != -1:
            # This is an expression group with a logical connective
            left_tokens = tokens[:op_idx]
            op = tokens[op_idx]
            right_tokens = tokens[op_idx+1:]
            
            # Parse the left side, which could be its own expression group
            left = parse_expression_group(left_tokens)
            
            # Special handling for all unary operations after logical connectives
            if (len(right_tokens) >= 2 and 
                right_tokens[0] in UNARY_OPERATION_SYMBOLS):
                
                # Find the closing parenthesis
                close_idx = -1
                paren_count = 1
                for i in range(2, len(right_tokens)):
                    if right_tokens[i] == '(':
                        paren_count += 1
                    elif right_tokens[i] == ')':
                        paren_count -= 1
                        if paren_count == 0:
                            close_idx = i
                            break
                
                if close_idx > 0:
                    # Extract the operand
                    operand_tokens = right_tokens[2:close_idx]
                    # Check if operand contains a logical operator
                    has_logical = False
                    paren_count = 0
                    for token in operand_tokens:
                        if token == '(':
                            paren_count += 1
                        elif token == ')':
                            paren_count -= 1
                        elif token in LOGICAL_CONNECTIVE_SYMBOLS and paren_count == 0:
                            has_logical = True
                            break
                    
                    if has_logical:
                        operand = parse_expression_group(operand_tokens)
                    else:
                        operand = parse_expression(operand_tokens)
                    
                    # Create the unary operation tuple
                    unary_op = (right_tokens[0], [operand])
                    
                    # Handle any remaining tokens after the unary operation
                    if close_idx + 1 < len(right_tokens):
                        remaining = right_tokens[close_idx+1:]
                        if remaining[0] in LOGICAL_CONNECTIVE_SYMBOLS:
                            # Another logical connective follows
                            next_right = parse_expression_group(remaining[1:])
                            return [left, op, [unary_op, remaining[0], next_right]]
                    
                    # Just the unary operation
                    return [left, op, unary_op]
            
            # Parse the right side normally, which could be its own expression group
            right = parse_expression_group(right_tokens)
            
            # Flatten both left and right sides if they have the same logical operator
            # E.g., [A, '⋀', B] ⋀ [C, '⋀', D] should become [A, '⋀', B, '⋀', C, '⋀', D]
            
            # Check if left side can be flattened
            if (isinstance(left, list) and len(left) >= 3 and 
                isinstance(left[1], str) and left[1] == op):
                # Left side has same operator - it's already flat
                # Check if right also needs flattening
                if (isinstance(right, list) and len(right) >= 3 and 
                    isinstance(right[1], str) and right[1] == op):
                    # Both sides have same operator - merge them all
                    return left + [op] + right
                else:
                    # Only left is flat, append right
                    return left + [op, right]
            elif (isinstance(right, list) and len(right) >= 3 and 
                  isinstance(right[1], str) and right[1] == op):
                # Only right side has same operator - flatten it
                return [left, op] + right
            else:
                # Neither side needs flattening - standard case
                return [left, op, right]
        else:
            # Handle nested expression groups
            if tokens and tokens[0] == '(' and tokens[-1] == ')':
                inner_tokens = tokens[1:-1]
                
                # Check if inner tokens contain logical operators
                inner_op_idx = find_logical_operator_index(inner_tokens)
                
                if inner_op_idx != -1:
                    # This is a nested expression group
                    left_tokens = inner_tokens[:inner_op_idx]
                    op = inner_tokens[inner_op_idx]
                    right_tokens = inner_tokens[inner_op_idx+1:]
                    left = parse_expression_group(left_tokens)
                    right = parse_expression_group(right_tokens)
                    
                    # Apply flattening logic for both left and right sides
                    if (isinstance(left, list) and len(left) >= 3 and 
                        isinstance(left[1], str) and left[1] == op):
                        # Left side has same operator
                        if (isinstance(right, list) and len(right) >= 3 and 
                            isinstance(right[1], str) and right[1] == op):
                            # Both sides have same operator
                            return left + [op] + right
                        else:
                            return left + [op, right]
                    elif (isinstance(right, list) and len(right) >= 3 and 
                          isinstance(right[1], str) and right[1] == op):
                        # Right side has the same operator - flatten it
                        return [left, op] + right
                    else:
                        return [left, op, right]
            
            # If no nested expression group, parse as a regular expression
            return parse_expression(tokens)

    def post_process_result(result):
        """
        Post-process the parsed result to fix any remaining issues with quantifier expressions
        and ensure consistent formatting of all expressions.
        """
        if result is None:
            return None
            
        # Helper function to recursively fix expressions
        def fix_expressions(structure):
            # Handle tuples (potential quantifier expressions)
            if isinstance(structure, tuple) and len(structure) >= 2:
                symbol, params = structure[0], structure[1]
                
                # Fix incorrectly formatted expressions
                if symbol == '@' and isinstance(params, list) and len(params) >= 2:
                    # Check for incorrectly formatted quantifier or unary operation
                    if params[0] in QUANTIFIER_SYMBOLS:
                        # Convert '@', ['∃', '_x'] to ('∃', ['_x'])
                        return (params[0], [params[1]])
                    elif params[0] in UNARY_OPERATION_SYMBOLS:
                        # Convert '@', ['↲', '_x'] to ('↲', ['_x'])
                        return (params[0], [params[1]])
                    # Handle potential nested unary operations with parentheses
                    elif isinstance(params[0], str) and any(params[0].startswith(op) for op in UNARY_OPERATION_SYMBOLS) and '(' in params[0]:
                        # Extract the operator and variable from "↲(_x)"
                        for op in UNARY_OPERATION_SYMBOLS:
                            if params[0].startswith(op):
                                var = params[0][len(op)+1:-1]  # Remove operator and parentheses
                                return (op, [var])
                
                # Ensure standalone quantifiers are formatted correctly
                elif symbol in QUANTIFIER_SYMBOLS:
                    # Make sure params is a list
                    if not isinstance(params, list):
                        params = [params]
                    # Process each parameter recursively
                    fixed_params = [fix_expressions(p) for p in params]
                    return (symbol, fixed_params)
                
                # Process other tuple structures
                if isinstance(params, list):
                    fixed_params = [fix_expressions(p) for p in params]
                    return (symbol, fixed_params)
                else:
                    return (symbol, fix_expressions(params))
            
            # Handle lists (potential expression groups or incorrectly formatted operations)
            elif isinstance(structure, list):
                # Check if this is a binary operation format like ['_nl', '≪', '_a']
                if (len(structure) == 3 and 
                    isinstance(structure[0], str) and 
                    isinstance(structure[1], str) and 
                    structure[1] in SET_OPERATION_SYMBOLS):
                    # Convert to proper binary operation format: ('≪', ['_nl', '_a'])
                    return (structure[1], [structure[0], structure[2]])
                # Check if this is an unary operation like ['↲', '_x'] in a logical connective
                elif (len(structure) >= 2 and 
                      isinstance(structure[0], str) and 
                      structure[0] in UNARY_OPERATION_SYMBOLS):
                    # Convert to proper unary operation format: ('↲', ['_x'])
                    return (structure[0], [fix_expressions(structure[1])])
                # Check for logical expression with a unary operation missing
                elif (len(structure) == 3 and
                      isinstance(structure[1], str) and 
                      structure[1] in LOGICAL_CONNECTIVE_SYMBOLS and
                      isinstance(structure[0], tuple) and
                      isinstance(structure[2], str) and
                      any(structure[2].startswith(op) for op in UNARY_OPERATION_SYMBOLS)):
                    # Fix the unary operation in the right side
                    for op in UNARY_OPERATION_SYMBOLS:
                        if structure[2].startswith(op):
                            if '(' in structure[2] and ')' in structure[2]:
                                # Extract the variable inside parentheses
                                var_start = structure[2].find('(') + 1
                                var_end = structure[2].rfind(')')
                                if var_start < var_end:
                                    var = structure[2][var_start:var_end]
                                    return [structure[0], structure[1], (op, [var])]
                # Process other list structures
                return [fix_expressions(item) for item in structure]
            
            # Special handling for strings that might contain unary operations
            elif isinstance(structure, str):
                # Check if this is a string with a unary operation pattern like "↲(_x)"
                for op in UNARY_OPERATION_SYMBOLS:
                    if structure.startswith(op) and '(' in structure and structure.endswith(')'):
                        # Extract the operand between parentheses
                        operand = structure[structure.find('(')+1:structure.rfind(')')]
                        if operand:
                            return (op, [operand])
                # Not a unary operation string
                return structure
            
            # Default case: return unchanged
            return structure
            
        # Apply expression fixes and return the result
        fixed_result = fix_expressions(result)
        return fixed_result

    try:
        # Tokenize the script
        tokens = tokenize(script)
        if not tokens:
            return {'ast': None, 'meta': None} if meta else None
        
        # Parse the tokens
        has_top_level_logical = False
        paren_count = 0
        
        for token in tokens:
            if token == '(':
                paren_count += 1
            elif token == ')':
                paren_count -= 1
            elif token in LOGICAL_CONNECTIVE_SYMBOLS and paren_count == 0:
                has_top_level_logical = True
                break
                
        # Parse according to the structure
        if has_top_level_logical:
            result = parse_expression_group(tokens)
        else:
            result = parse_expression(tokens)
        
        # Post-process to fix any remaining issues with quantifier expressions
        result = post_process_result(result)
        if not meta:
            return result

        # Build metadata in a separate pass while preserving AST exactly.
        _, token_spans = tokenize_with_spans(script)
        meta_tree = build_metadata_tree(result, script, token_spans, [], [0])
        return {
            'ast': result,
            'meta': meta_tree,
        }
        
    except Exception as e:
        print(f"Parsing Error: {str(e)}")
        return {'ast': None, 'meta': None} if meta else None

# Example usage
if __name__ == "__main__":
    test_scripts = [
        # Basic case with standalone quantifier
        "(∃(_x))",
        
        # Quantifier with assignment in logical expression
        "(∃(_x) ⋀ (_z ≔ (_x + 1)))",
        
        # Complex nested case
        "((∃(_x) ⋀ (_z ≔ (_x + 1)) ⋀ ((∃(_x) ⋀ ↲(_x)) ⋁ ↲(_z))) ⋁ true) ⋀ (_y ≔ _t[5])",
        
        # Very long nested expression with multiple parts
        "((∃(_x) ⋀ (_z ≔ (_x + 1)) ⋀ ((∃(_x) ⋀ ↲(_x)) ⋁ ↲(_z))) ⋁ true) ⋀ (_y ≔ _t[5]) ⋀ ((∃(_l) ⋀ (_nl ≔ []) ⋀ ((∃(_a) ∈ _l) ⋀ (_nl ≪ _a)) ⋀ ↲(_nl)) ⋁ ↲(Ø))"
    ]
    
    for i, script in enumerate(test_scripts):
        print(f"\nScript {i+1}: {script}")
        result = parse_script(script)
        print(f"Result: {result}")