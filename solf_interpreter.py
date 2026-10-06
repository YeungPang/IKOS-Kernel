import re
import json
import copy
import os
import sys
import importlib
from collections import deque
from typing import Dict, List, Tuple, Set, Any, Union, Optional

class SOLFReturnValue:
    """Internal class to represent return values without using exceptions."""
    def __init__(self, value):
        self.value = value
        self.is_return = True
    
    def __str__(self):
        return f"Return({self.value})"
    
    def __repr__(self):
        return f"SOLFReturnValue({self.value})"

class Clause:
    """Represents a SOLF clause with predicates and execution state."""
    
    def __init__(self, 
                 name: str,
                 predicates: Union[Any, List[Any]],
                 arguments: Optional[List[Any]] = None,
                 associated_object: Optional[Any] = None):
        """
        Initialize a SOLF clause.
        
        Args:
            name: The name of the clause.
            predicates: A single predicate or list of predicates (output from solf parser).
            arguments: List of arguments for the clause. Some arguments are atoms for inferencing
                      and won't be added to variables, while others are input values.
            associated_object: An optional object this clause is associated with.
        """
        self.name = name
        self.predicates = predicates if isinstance(predicates, list) else [predicates]
        self.variables: Dict[str, Any] = {}
        self.arguments = arguments or []
        self.associated_object = associated_object
        self.execution_indices: List[int] = [0]  # Track execution progress through nested predicates
        self.last_failed_indices: List[int] = []  # <-- add this line
    
    def initialize_arguments(self, arg_values: Optional[List[Any]] = None):
        """
        Initialize clause variables from argument values.
        
        Args:
            arg_values: List of values to assign to arguments.
                       Atoms (used for inferencing) are not added to variables.
        """
        if arg_values and self.arguments:
            for i, (arg, value) in enumerate(zip(self.arguments, arg_values)):
                # Only add to variables if argument is a variable (starts with _)
                if isinstance(arg, str) and arg.startswith('_'):
                    self.variables[arg] = value
    
    def reset_execution(self):
        """Reset execution indices to start from the beginning."""
        self.execution_indices = [0]
        self.last_failed_indices = []  # <-- add this line
    
    def save_state(self) -> Dict[str, Any]:
        """Save current variable state for backtracking."""
        return copy.deepcopy(self.variables)
    
    def restore_state(self, state: Dict[str, Any]):
        """Restore variable state for backtracking."""
        self.variables = copy.deepcopy(state)
    
    def __repr__(self):
        return f"Clause(name='{self.name}', predicates={len(self.predicates)}, vars={len(self.variables)})"


class DebugSession:
    """Collects interpreter debug events and exposes step-based inspection."""

    def __init__(self):
        # Breakpoints persist across reset() calls — they describe where to pause,
        # not state about a particular run.
        self.breakpoints: set = set()             # set of tuple path (root-frame paths)
        self.named_breakpoints: set = set()       # set of clause name strings
        self.named_path_breakpoints: set = set()  # set of (clause_name, tuple_path)
        self.reset()

    def reset(self, ast=None, meta=None):
        self.ast = ast
        self.meta = meta
        self.trace = []
        self.current_index = -1
        self.current_node = None
        self.current_path = []
        self.current_environment = {}
        self.call_stack = []
        self.last_result = None
        self.final_result = None
        self.finished = False
        self.paused = False
        self._pending_environment = {}
        self._pending_call_stack = []
        self._pending_last_result = None
        self._final_environment = {}
        self._final_call_stack = []

    def update_runtime_state(self, environment, call_stack, last_result):
        self._pending_environment = copy.deepcopy(environment)
        self._pending_call_stack = list(call_stack)
        self._pending_last_result = copy.deepcopy(last_result)

    def _record_event(self, kind, meta, result_marker=None):
        meta_copy = copy.deepcopy(meta) if meta is not None else None
        event = {
            'kind': kind,
            'meta': meta_copy,
            'environment': copy.deepcopy(self._pending_environment),
            'call_stack': list(self._pending_call_stack),
            'last_result': copy.deepcopy(self._pending_last_result),
            'result': copy.deepcopy(result_marker),
        }
        self.trace.append(event)
        return event

    def before_eval(self, meta):
        self._record_event('before', meta)

    def after_eval(self, meta, result):
        self._record_event('after', meta, result)

    def on_return(self, meta, value):
        self._record_event('return', meta, value)

    def finish(self, environment, call_stack, result):
        self._final_environment = copy.deepcopy(environment)
        self._final_call_stack = list(call_stack)
        self.final_result = copy.deepcopy(result)

    def _event_depth(self, event):
        meta = event.get('meta') or {}
        path = meta.get('path', []) if isinstance(meta, dict) else []
        return (len(event.get('call_stack', [])), len(path))

    def _apply_event(self, event):
        meta = event.get('meta')
        self.current_node = meta
        self.current_path = list(meta.get('path', [])) if isinstance(meta, dict) else []
        self.current_environment = copy.deepcopy(event.get('environment', {}))
        self.call_stack = list(event.get('call_stack', []))
        self.last_result = copy.deepcopy(event.get('last_result'))
        self.paused = True
        self.finished = False

    def _find_next_before(self, start_idx, mode):
        if not self.trace:
            return None

        if start_idx < 0:
            for idx, event in enumerate(self.trace):
                if event.get('kind') == 'before':
                    return idx
            return None

        base_event = self.trace[start_idx]
        base_frame_depth, base_path_depth = self._event_depth(base_event)

        for idx in range(start_idx + 1, len(self.trace)):
            event = self.trace[idx]
            if event.get('kind') != 'before':
                continue

            frame_depth, path_depth = self._event_depth(event)

            if mode == 'into':
                return idx

            if mode == 'over':
                if frame_depth < base_frame_depth:
                    return idx
                if frame_depth == base_frame_depth and path_depth <= base_path_depth:
                    return idx

            if mode == 'out':
                if frame_depth < base_frame_depth:
                    return idx
                if frame_depth == base_frame_depth and path_depth < base_path_depth:
                    return idx

        return None

    # ------------------------------------------------------------------
    # Breakpoint management
    # ------------------------------------------------------------------

    def add_breakpoint(self, path: tuple) -> None:
        """Register a root-frame AST-path breakpoint, e.g. (0,) or (0, 1, 2)."""
        self.breakpoints.add(tuple(path))

    def remove_breakpoint(self, path: tuple) -> None:
        """Remove a root-frame path breakpoint (no-op if not present)."""
        self.breakpoints.discard(tuple(path))

    def add_named_breakpoint(self, name: str) -> None:
        """Register a clause-name breakpoint — fires on any node inside that clause."""
        self.named_breakpoints.add(name)

    def remove_named_breakpoint(self, name: str) -> None:
        """Remove a clause-name breakpoint (no-op if not present)."""
        self.named_breakpoints.discard(name)

    def add_named_path_breakpoint(self, name: str, path: tuple) -> None:
        """Register a combined breakpoint: fires inside *name* at the given AST path."""
        self.named_path_breakpoints.add((name, tuple(path)))

    def remove_named_path_breakpoint(self, name: str, path: tuple) -> None:
        """Remove a combined clause-name + path breakpoint (no-op if not present)."""
        self.named_path_breakpoints.discard((name, tuple(path)))

    def clear_breakpoints(self) -> None:
        """Remove all registered breakpoints of every type."""
        self.breakpoints.clear()
        self.named_breakpoints.clear()
        self.named_path_breakpoints.clear()

    def _is_breakpoint_event(self, event: dict) -> bool:
        """Return True if *event* matches any registered breakpoint.

        Only ``'before'`` events are eligible — breakpoints pause *before*
        a node is evaluated, mirroring how debuggers in other languages work.

        Matching rules
        --------------
        Type 1 — path only (root frame):
            ``breakpoints`` contains ``tuple(meta['path'])`` and the event is in
            the root frame (call_stack == ['<root>']).

        Type 2 — clause name only:
            ``named_breakpoints`` contains the *innermost* frame in call_stack
            (i.e. ``call_stack[-1]``).  This fires on the very first node
            evaluated inside the named clause.

        Type 3 — clause name + path:
            ``named_path_breakpoints`` contains ``(call_stack[-1], tuple(meta['path']))``.
        """
        if event.get('kind') != 'before':
            return False

        meta = event.get('meta') or {}
        raw_path = meta.get('path', []) if isinstance(meta, dict) else []
        path_key = tuple(raw_path)
        call_stack = event.get('call_stack') or []
        current_frame = call_stack[-1] if call_stack else None

        # Type 1: root-frame path breakpoint
        if path_key in self.breakpoints and call_stack == ['<root>']:
            return True

        # Type 2: clause-name breakpoint
        if current_frame is not None and current_frame in self.named_breakpoints:
            return True

        # Type 3: clause-name + path breakpoint
        if current_frame is not None and (current_frame, path_key) in self.named_path_breakpoints:
            return True

        return False

    def continue_to_breakpoint(self):
        """Advance through the recorded trace until the next breakpoint is hit.

        If a registered breakpoint matches an event *after* the current position,
        the session pauses there (``paused=True``) and returns the state dict just
        as ``step()`` would.

        If no breakpoint is found before the end of the trace the session is
        finished and ``continue_run()`` semantics apply — returns ``final_result``
        and sets ``finished=True``.

        Breakpoints are checked against ``'before'`` events only.  The current
        position (``current_index``) itself is never rechecked — the search
        starts at ``current_index + 1``.
        """
        search_start = max(self.current_index + 1, 0)
        for idx in range(search_start, len(self.trace)):
            if self._is_breakpoint_event(self.trace[idx]):
                self.current_index = idx
                self._apply_event(self.trace[idx])
                return self.get_state()

        # No breakpoint hit — fall through to completion
        return self.continue_run()

    def step(self, mode='into'):
        next_idx = self._find_next_before(self.current_index, mode)
        if next_idx is None:
            self.current_node = None
            self.current_path = []
            self.current_environment = copy.deepcopy(self._final_environment)
            self.call_stack = list(self._final_call_stack)
            self.last_result = copy.deepcopy(self.final_result)
            self.finished = True
            self.paused = False
            return self.final_result

        self.current_index = next_idx
        self._apply_event(self.trace[next_idx])
        return self.get_state()

    def continue_run(self):
        self.current_index = len(self.trace) - 1 if self.trace else -1
        self.current_node = None
        self.current_path = []
        self.current_environment = copy.deepcopy(self._final_environment)
        self.call_stack = list(self._final_call_stack)
        self.last_result = copy.deepcopy(self.final_result)
        self.finished = True
        self.paused = False
        return self.final_result

    def get_state(self):
        return {
            'current_node': copy.deepcopy(self.current_node),
            'environment': copy.deepcopy(self.current_environment),
            'call_stack': list(self.call_stack),
            'last_result': copy.deepcopy(self.last_result),
            'finished': bool(self.finished),
        }

class SOLFInterpreter:
    """
    Interpreter for SOLF (Set, Object, Logic, Function) language.
    This class handles the execution of SOLF expressions and predicates.
    """
    
    def __init__(self):
        """Initialize the SOLF interpreter with empty context."""
        self.variables = {}
        self.objects = {}
        self.facts = {}
        self.clauses = {}
        self.predefined_functions = {
            'τ': self._builtin_trace,
            'ℛ': self._builtin_resource,
            '⊤': self._builtin_text_lookup,
            'sort': self._builtin_sort,
        }
        # Parser for clause definitions (if needed)
        self.parser = None
        # Flag used by ∃ operator to stop clause backtracking on first success
        self._stop_on_first = False
        # Recursion safety guards for clause invocation
        self.max_clause_call_depth = 1000
        self.max_same_clause_calls = 400
        self._clause_call_stack = []
        self._clause_call_counts = {}
        self._failed_clause_signatures = set()
        self._successful_clause_results = {}
        self._exist_choice_offsets = {}
        # Monotonic id so nested/backtracked clause attempts each get a unique SAVEPOINT name.
        self._savepoint_seq = 0
        # NOTE: A flag like `use_builtin_clause_solver` could be used in the future
        # to allow callers to plug in a deterministic built-in solver for any named
        # clause, keeping the interpreter generic.  The river_crossing-specific
        # implementation below has been commented out for that reason.
        # self.use_builtin_river_crossing_solver = False
        # Internal diagnostic logging switch ([DEBUG]/[FAIL] messages).
        self.debug_enabled = True
        self._active_debugger = None
        self._debug_meta_stack = []
        self._debug_path_stack = []
        self._debug_frame_labels = []
        self._debug_last_result = None
        self.trace_messages = []
    
    def set_parser(self, parser):
        """Set the parser for parsing clause bodies."""
        self.parser = parser

    def set_debug(self, enabled: bool):
        """Enable/disable interpreter internal diagnostics."""
        self.debug_enabled = bool(enabled)

    def _internal_log(self, message: str):
        """Print internal diagnostics only when debug logging is enabled."""
        if self.debug_enabled:
            print(message, flush=True)

    def _freeze_call_value(self, value):
        """Convert nested structures to a hashable canonical form."""
        if isinstance(value, dict):
            items = [
                (self._freeze_call_value(k), self._freeze_call_value(v))
                for k, v in value.items()
            ]
            return ('dict', tuple(sorted(items, key=lambda item: repr(item))))
        if isinstance(value, set):
            frozen = [self._freeze_call_value(v) for v in value]
            return ('set', tuple(sorted(frozen, key=repr)))
        if isinstance(value, list):
            return ('list', tuple(self._freeze_call_value(v) for v in value))
        if isinstance(value, tuple):
            return ('tuple', tuple(self._freeze_call_value(v) for v in value))
        try:
            hash(value)
            return value
        except TypeError:
            return ('repr', repr(value))

    def _build_call_signature(self, clause_name, call_args):
        frozen_args = tuple(self._freeze_call_value(arg) for arg in (call_args or []))
        return (clause_name, frozen_args)

    # ------------------------------------------------------------------
    # Python-function extension lookup (solf_function.py / domain_function.py)
    # ------------------------------------------------------------------
    _python_extension_modules = {}

    @classmethod
    def _load_python_extension_module(cls, module_name):
        """Load (or reload) a Python extension module from the same directory as this file."""
        if module_name in cls._python_extension_modules:
            return cls._python_extension_modules[module_name]
        module_dir = os.path.dirname(os.path.abspath(__file__))
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)
        try:
            cls._python_extension_modules[module_name] = importlib.import_module(module_name)
        except ImportError:
            cls._python_extension_modules[module_name] = None
        return cls._python_extension_modules[module_name]

    def _try_python_function(self, func_name: str, call_args) -> Any:
        """
        Try to call a Python function named `func_name` from registered extension modules
        with the given resolved arguments.

        Returns:
            The function's return value if it succeeds (even if the value is
            falsy, it is returned as-is so the caller can distinguish between
            a successful result of 0/False and a genuine clause failure).
            Returns None when no matching function is found, and False when
            the function raises an exception.
        """
        args = call_args if call_args is not None else []
        for module_name in ("solf_function", "domain_function"):
            module = self._load_python_extension_module(module_name)
            if module is None:
                continue
            fn = getattr(module, func_name, None)
            if fn is None or not callable(fn):
                continue
            try:
                result = fn(*args)
                self._internal_log(f"[DEBUG] python_function_module={module_name} python_function={func_name} args={args} result={result}")
                return result
            except Exception as exc:
                self._internal_log(f"[FAIL] python_function_module={module_name} python_function={func_name} reason=exception exception={exc}")
                return False
        return None

    def _predicate_has_side_effects(self, predicate: Any) -> bool:
        """Detect assignment-like operators that mutate interpreter/clause state."""
        if isinstance(predicate, tuple) and len(predicate) == 2:
            operator, operands = predicate
            if operator in ['≔', '⥹', '⥻', '≪']:
                return True
            if isinstance(operands, list):
                return any(self._predicate_has_side_effects(op) for op in operands)
            return self._predicate_has_side_effects(operands)

        if isinstance(predicate, list):
            return any(self._predicate_has_side_effects(item) for item in predicate)

        return False

    def _clause_defs_are_pure(self, clause_defs: List[Dict[str, Any]]) -> bool:
        for clause_def in clause_defs:
            if self._predicate_has_side_effects(clause_def.get('parsed_body')):
                return False
        return True
        
    def _extract_clause_body_from_parsed(self, parsed):
        """
        If parser returns full-clause AST like ('⦃⦄', [args, body]),
        extract body. Otherwise return parsed as-is.
        """
        if parsed is None:
            return None

        if isinstance(parsed, tuple) and len(parsed) == 2:
            tag, content = parsed
            if tag in ('⦃⦄', '⦃', 'clause') and isinstance(content, list) and len(content) >= 2:
                return content[1]

        return parsed

    def _extract_ast_and_meta(self, parsed, metadata=None):
        """Normalize parser output so callers can pass AST-only or {'ast','meta'} payloads."""
        if isinstance(parsed, dict) and 'ast' in parsed:
            return parsed.get('ast'), parsed.get('meta')
        return parsed, metadata

    def _infer_debug_node_type(self, predicate: Any) -> str:
        if isinstance(predicate, list):
            if any(isinstance(item, str) and item in ['⋀', '⋁'] for item in predicate):
                return 'chain'
            return 'list'
        if isinstance(predicate, tuple) and len(predicate) == 2:
            operator = predicate[0]
            if operator in ['≔', '⥻', '⥹', '≪']:
                return 'assignment'
            if operator in ['∀', '∃', '∄']:
                return 'quantifier'
            if operator in ['↲', '¬', '#', 'ℰ']:
                return 'unary'
            return 'predicate'
        if isinstance(predicate, (int, float, bool)):
            return 'literal'
        if isinstance(predicate, str):
            return 'identifier'
        return 'node'

    def _lookup_debug_meta(self, path: List[int]):
        if not self._debug_meta_stack:
            return None
        node = self._debug_meta_stack[-1]
        if not isinstance(node, dict):
            return None
        for index in path:
            children = node.get('children')
            if not isinstance(children, list) or index < 0 or index >= len(children):
                return None
            node = children[index]
        return node

    def _build_synthetic_meta(self, predicate: Any, path: List[int]):
        return {
            'start': None,
            'end': None,
            'path': list(path),
            'node_type': self._infer_debug_node_type(predicate),
            'text': repr(predicate),
        }

    def _current_debug_meta(self, predicate: Any):
        if not self._active_debugger or not self._debug_path_stack:
            return None
        path = list(self._debug_path_stack[-1])
        meta = self._lookup_debug_meta(path)
        if meta is not None:
            return meta
        return self._build_synthetic_meta(predicate, path)

    def _snapshot_environment(self, clause: Optional['Clause'] = None):
        source = clause.variables if clause else self.variables
        return copy.deepcopy(source)

    def _snapshot_call_stack(self):
        return list(self._debug_frame_labels)

    def _sync_debugger_state(self, clause: Optional['Clause'] = None):
        if self._active_debugger is None:
            return
        if hasattr(self._active_debugger, 'update_runtime_state'):
            self._active_debugger.update_runtime_state(
                self._snapshot_environment(clause),
                self._snapshot_call_stack(),
                self._debug_last_result,
            )

    def _debug_before_eval(self, predicate: Any, clause: Optional['Clause'] = None):
        if self._active_debugger is None:
            return
        meta = self._current_debug_meta(predicate)
        self._sync_debugger_state(clause)
        self._active_debugger.before_eval(meta)

    def _debug_after_eval(self, predicate: Any, result: Any, clause: Optional['Clause'] = None):
        if self._active_debugger is None:
            return
        self._debug_last_result = result.value if isinstance(result, SOLFReturnValue) else result
        meta = self._current_debug_meta(predicate)
        self._sync_debugger_state(clause)
        self._active_debugger.after_eval(meta, self._debug_last_result)

    def _debug_on_return(self, predicate: Any, value: Any, clause: Optional['Clause'] = None):
        if self._active_debugger is None:
            return
        self._debug_last_result = value
        meta = self._current_debug_meta(predicate)
        self._sync_debugger_state(clause)
        self._active_debugger.on_return(meta, value)

    def _push_debug_path(self, path_suffix: List[int]):
        if self._active_debugger is None or not self._debug_path_stack or not path_suffix:
            return 0
        self._debug_path_stack[-1].extend(path_suffix)
        return len(path_suffix)

    def _pop_debug_path(self, count: int):
        if self._active_debugger is None or not self._debug_path_stack or count <= 0:
            return
        del self._debug_path_stack[-1][-count:]

    def _evaluate_child(self, predicate: Any, clause: Optional['Clause'] = None, path_suffix: Optional[List[int]] = None):
        count = self._push_debug_path(path_suffix or [])
        try:
            return self._execute_predicate_internal(predicate, clause)
        finally:
            self._pop_debug_path(count)

    def _push_debug_frame(self, label: str, meta_root: Optional[Dict[str, Any]] = None):
        if self._active_debugger is None:
            return False
        self._debug_frame_labels.append(label)
        self._debug_meta_stack.append(meta_root)
        self._debug_path_stack.append([])
        return True

    def _pop_debug_frame(self):
        if self._active_debugger is None:
            return
        if self._debug_frame_labels:
            self._debug_frame_labels.pop()
        if self._debug_meta_stack:
            self._debug_meta_stack.pop()
        if self._debug_path_stack:
            self._debug_path_stack.pop()

    def run(self, ast: Any, clause: Optional['Clause'] = None, debugger=None, metadata=None):
        """Run a parsed AST in normal or traced debug mode."""
        predicate, meta = self._extract_ast_and_meta(ast, metadata)
        self.trace_messages = []

        if debugger is None:
            result = self._execute_predicate_internal(predicate, clause)
            return result.value if isinstance(result, SOLFReturnValue) else result

        debugger.reset(predicate, meta)
        self._active_debugger = debugger
        self._debug_meta_stack = [meta]
        self._debug_path_stack = [[]]
        self._debug_frame_labels = ['<root>']
        self._debug_last_result = None

        try:
            result = self._execute_predicate_internal(predicate, clause)
            final_value = result.value if isinstance(result, SOLFReturnValue) else result
            debugger.finish(self._snapshot_environment(clause), self._snapshot_call_stack(), final_value)
            return final_value
        finally:
            self._active_debugger = None
            self._debug_meta_stack = []
            self._debug_path_stack = []
            self._debug_frame_labels = []
            self._debug_last_result = None

    def step(self, ast: Any = None, debugger=None, mode: str = 'into'):
        """Advance one debug step using a recorded debug session."""
        if debugger is None:
            debugger = DebugSession()
        if not isinstance(debugger, DebugSession):
            raise TypeError('step() requires a DebugSession debugger')
        if not debugger.trace:
            if ast is None:
                raise ValueError('step() requires an AST the first time it is called')
            self.run(ast, debugger=debugger)
        return debugger.step(mode)

    def continue_run(self, ast: Any = None, debugger=None):
        """Fast-forward a debug session to completion."""
        if debugger is None:
            debugger = DebugSession()
        if not isinstance(debugger, DebugSession):
            raise TypeError('continue_run() requires a DebugSession debugger')
        if not debugger.trace:
            if ast is None:
                raise ValueError('continue_run() requires an AST the first time it is called')
            return self.run(ast, debugger=debugger)
        return debugger.continue_run()

    def get_state(self, debugger=None):
        """Return current debug state for an attached DebugSession."""
        if debugger is None:
            return {
                'current_node': None,
                'environment': copy.deepcopy(self.variables),
                'call_stack': [],
                'last_result': None,
                'finished': True,
            }
        if not isinstance(debugger, DebugSession):
            raise TypeError('get_state() requires a DebugSession debugger')
        return debugger.get_state()
    
    def execute_predicate(self, predicate: Any, clause: Optional['Clause'] = None, debugger=None, metadata=None) -> Any:
        """
        Execute a SOLF predicate and return the result.
        
        A predicate can be in one of these forms:
        - Binary operation: (operator, [left_operand, right_operand])
        - Unary operation: (operator, [operand])
        - Function call: (function_name, [arg1, arg2, ...])
        - Expression group: [left_expr, logical_op, right_expr] for logical connections
        
        Args:
            predicate: The predicate to execute.
            clause: The clause instance containing variable bindings and execution state (optional).
            
        Returns:
            The result of executing the predicate.
        """
        if debugger is not None or metadata is not None or (isinstance(predicate, dict) and 'ast' in predicate):
            return self.run(predicate, clause=clause, debugger=debugger, metadata=metadata)

        result = self._execute_predicate_internal(predicate, clause)

        # If result is a return value, extract and return the actual value
        if isinstance(result, SOLFReturnValue):
            return result.value

        return result
    
    def _execute_predicate_chain(self, predicates: List[Any], clause: Optional['Clause'] = None) -> Any:
        """
        Execute a chain of predicates linked by ⋀ (AND) or ⋁ (OR).
        
        Implements transactional semantics:
        - Save state at beginning of chain
        - Restore state if chain fails
        - For OR chains, restore state before trying alternatives
        - Only commit changes if chain succeeds
        
        Args:
            predicates: List of predicates and logical operators [pred1, '⋀', pred2, '⋁', pred3, ...]
            clause: The clause instance containing variable bindings.
            
        Returns:
            The result of executing the predicate chain.
        """
        if not predicates:
            return True
        
        # Single predicate, no chain
        if len(predicates) == 1:
            return self._evaluate_child(predicates[0], clause, [0])
        
        # Save state at the beginning of this chain
        if clause:
            chain_start_state = clause.save_state()
        else:
            chain_start_state = copy.deepcopy(self.variables)
        
        # Handle three-part logical expressions: [left, operator, right]
        if len(predicates) == 3 and predicates[1] in ['⋀', '⋁']:
            left_expr, logical_op, right_expr = predicates
            
            if logical_op == '⋀':  # AND
                result = self._evaluate_child(left_expr, clause, [0])
                
                # Check if left result is a return value - propagate immediately
                if isinstance(result, SOLFReturnValue):
                    return result
                
                # Short-circuit: if left fails (explicit False), don't execute right
                # Note: Empty collections, 0,  etc. should NOT cause failure, only False
                if result is False:
                    if clause:
                        clause.last_failed_indices = [0]
                        self._internal_log(f"[FAIL] clause={clause.name} predicate[0]={left_expr!r} execution_indices=[0]")
                    # Restore state on AND chain failure
                    if clause:
                        clause.restore_state(chain_start_state)
                    else:
                        self.variables = chain_start_state
                    return False
                
                # Left succeeded - update progress before trying right
                if clause:
                    clause.execution_indices = [0]
                
                right_result = self._evaluate_child(right_expr, clause, [2])
                
                # Check if right result is a return value
                if isinstance(right_result, SOLFReturnValue):
                    return right_result
                
                # Check if AND chain succeeded or failed
                success = (result is not False) and (right_result is not False)
                
                if not success:
                    if clause:
                        clause.last_failed_indices = [1]
                        self._internal_log(f"[FAIL] clause={clause.name} predicate[1]={right_expr!r} execution_indices=[1]")
                    # Restore state on failure
                    if clause:
                        clause.restore_state(chain_start_state)
                    else:
                        self.variables = chain_start_state
                    return False
                
                # Both succeeded - update progress
                if clause:
                    clause.execution_indices = [1]
                # Both succeeded, keep the state changes
                return True
                
            elif logical_op == '⋁':  # OR
                left_result = self._evaluate_child(left_expr, clause, [0])
                
                # Check if left result is a return value
                if isinstance(left_result, SOLFReturnValue):
                    return left_result
                
                # Only explicit False means failure; keep 0/empty collections as valid results.
                if left_result is not False:
                    return left_result
                
                # Left failed, restore state before trying right alternative
                if clause:
                    clause.restore_state(chain_start_state)
                else:
                    self.variables = chain_start_state
                
                # Try right alternative
                right_result = self._evaluate_child(right_expr, clause, [2])
                
                # Check if right result is a return value
                if isinstance(right_result, SOLFReturnValue):
                    return right_result
                
                # Only explicit False means failure; keep 0/empty collections as valid results.
                if right_result is not False:
                    return right_result
                
                # Both alternatives failed, restore to original state
                if clause:
                    clause.restore_state(chain_start_state)
                else:
                    self.variables = chain_start_state
                return False
        
        # Handle longer chains: [op1, '⋀', op2, '⋀', op3, ...]
        # All operators must be the same (all ⋀ or all ⋁)
        if len(predicates) >= 3 and all(predicates[i] == '⋀' for i in range(1, len(predicates), 2)):
            # Detect quantifier-style iteration embedded in AND chains.
            # Expected form in parsed AST: ('∀'|'∃', [var_name, collection_expr]) ⋀ body...
            for i in range(0, len(predicates), 2):
                if i >= len(predicates):
                    break
                candidate = predicates[i]
                if (
                    isinstance(candidate, tuple)
                    and len(candidate) == 2
                    and candidate[0] in ['∀', '∃']
                ):
                    quantifier_op, operands = candidate
                    variable_expr = operands[0] if isinstance(operands, list) and len(operands) == 2 else None
                    is_supported_variable = (
                        isinstance(variable_expr, str) and variable_expr.startswith('_')
                    ) or (
                        isinstance(variable_expr, tuple)
                        and len(variable_expr) == 2
                        and variable_expr[0] == '𝒯'
                    )
                    if (
                        isinstance(operands, list)
                        and len(operands) == 2
                        and is_supported_variable
                    ):
                        variable = variable_expr
                        collection_expr = operands[1]

                        # Evaluate predicates before quantifier first.
                        if i > 0:
                            before_predicates = predicates[:i]
                            for j in range(0, len(before_predicates), 2):
                                if j < len(before_predicates):
                                    pred_idx = j // 2
                                    result = self._evaluate_child(before_predicates[j], clause, [j])
                                    if isinstance(result, SOLFReturnValue):
                                        return result
                                    if result is False:
                                        if clause:
                                            clause.last_failed_indices = [pred_idx]
                                            self._internal_log(f"[FAIL] clause={clause.name} predicate[{pred_idx}]={before_predicates[j]!r} execution_indices=[{pred_idx}]")
                                            clause.restore_state(chain_start_state)
                                        else:
                                            self.variables = chain_start_state
                                        return False

                        collection = self._evaluate_child(collection_expr, clause, [i, 1, 1])
                        body_predicates = predicates[i + 2:] if i + 2 < len(predicates) else []
                        iter_result = self._execute_iteration(
                            quantifier_op,
                            variable,
                            collection,
                            body_predicates,
                            clause,
                        )

                        if iter_result is False and clause:
                            # First body predicate index after quantifier in the local chain view
                            clause.last_failed_indices = [max((i // 2), 0)]
                            clause.restore_state(chain_start_state)
                        elif iter_result is False:
                            self.variables = chain_start_state

                        return iter_result
            
            # No iteration found - execute as normal AND chain
            last_result = True
            for i in range(0, len(predicates), 2):  # Skip logical operators
                pred_idx = i // 2
                if i < len(predicates):
                    result = self._evaluate_child(predicates[i], clause, [i])
                    
                    # Check for return values - terminate immediately
                    if isinstance(result, SOLFReturnValue):
                        return result
                    
                    # Fail on first explicit False (not falsy values)
                    if result is False:
                        if clause:
                            clause.last_failed_indices = [pred_idx]
                            self._internal_log(f"[FAIL] clause={clause.name} predicate[{pred_idx}]={predicates[i]!r} execution_indices=[{pred_idx}]")
                        # Restore state on AND chain failure
                        if clause:
                            clause.restore_state(chain_start_state)
                        else:
                            self.variables = chain_start_state
                        return False
                    
                    # Success - update execution progress
                    if clause:
                        clause.execution_indices = [pred_idx]
                    last_result = result
            # Return True if all predicates succeeded (none returned False)
            return True if last_result is not False else False
        
        # For mixed or complex chains, process recursively
        # If we can't determine structure, treat as single expression to evaluate
        if len(predicates) == 1:
            return self._evaluate_child(predicates[0], clause, [0])
        
        # Default: treat entire list as unrecognized structure, return as-is
        return predicates
    
    def _execute_iteration(self, 
                          quantifier: str,
                          variable: Union[str, Tuple],
                          collection: Any,
                          body_predicates: Any,
                          clause: Optional['Clause'] = None) -> Any:
        """
        Execute iteration with proper backtracking and state management.
        
        Args:
            quantifier: '∀' (for all) or '∃' (there exists)
            variable: Iteration variable (string) or tuple of variables
            collection: Collection to iterate over
            body_predicates: Predicates to execute for each element
            clause: The clause instance
            
        Returns:
            Success/failure of iteration according to quantifier semantics.
        """
        # Save state at iteration start
        if clause:
            iteration_start_state = clause.save_state()
        else:
            iteration_start_state = copy.deepcopy(self.variables)

        context = clause.variables if clause else self.variables
        iter_var_names: List[str] = []
        if isinstance(variable, tuple) and len(variable) == 2 and variable[0] == '𝒯':
            iter_var_names = [v for v in variable[1] if isinstance(v, str)]
        elif isinstance(variable, str):
            iter_var_names = [variable]

        saved_iter_bindings = {}
        for name in iter_var_names:
            if name in context:
                saved_iter_bindings[name] = (True, copy.deepcopy(context[name]))
            else:
                saved_iter_bindings[name] = (False, None)

        def restore_iter_bindings():
            target = clause.variables if clause else self.variables
            for name, (existed, old_value) in saved_iter_bindings.items():
                if existed:
                    target[name] = copy.deepcopy(old_value)
                else:
                    target.pop(name, None)
        
        # Ensure collection is iterable
        if not isinstance(collection, (list, tuple, set, dict)):
            restore_iter_bindings()
            return False if quantifier == '∀' else False
        
        # Handle empty collection
        if not collection:
            restore_iter_bindings()
            return True if quantifier == '∀' else False
        
        # Determine if variable is pre-bound (filtering)
        pre_bound_filter = None
        if isinstance(variable, str):
            if variable in context:
                pre_bound_filter = context[variable]
        
        # Prepare iteration items
        if isinstance(collection, dict):
            items = list(collection.items())
        else:
            items = list(collection)

        # Choice-point key for existential backtracking: lets repeated invocations
        # try the next successful candidate instead of always the first.
        choice_key = None
        success_skip = 0
        if quantifier == '∃':
            scope_snapshot = clause.save_state() if clause else copy.deepcopy(self.variables)
            choice_key = (
                clause.name if clause else '<global>',
                self._freeze_call_value(variable),
                self._freeze_call_value(collection),
                repr(body_predicates),
                self._freeze_call_value(scope_snapshot),
            )
            success_skip = self._exist_choice_offsets.get(choice_key, 0)
            success_seen = 0
        
        success_count = 0
        
        for item in items:
            # Apply pre-bound filtering
            if pre_bound_filter is not None:
                if isinstance(variable, tuple):
                    # Tuple matching for filtering
                    if item != pre_bound_filter:
                        continue
                else:
                    # Single variable filtering
                    if item != pre_bound_filter:
                        continue
            
            # For ∃, each candidate is an alternative branch. Roll back failed/skipped
            # candidate effects before trying the next candidate.
            if clause:
                per_item_state = clause.save_state()
            else:
                per_item_state = copy.deepcopy(self.variables)
            
            # Bind iteration variable(s)
            if isinstance(variable, tuple) and variable[0] == '𝒯':
                # Tuple unpacking: (∀(_k, _v) ∈ _map)
                var_names = variable[1]
                if isinstance(item, (tuple, list)) and len(var_names) == len(item):
                    for i, var_name in enumerate(var_names):
                        if isinstance(var_name, str):
                            if clause:
                                clause.variables[var_name] = item[i]
                            else:
                                self.variables[var_name] = item[i]
                else:
                    # Mismatch in tuple size, skip this item
                    continue
            else:
                # Single variable binding
                if clause:
                    clause.variables[variable] = item
                else:
                    self.variables[variable] = item
            
            # Execute body predicates
            result = self._evaluate_child(body_predicates, clause, [2])
            
            # Check for return - terminates entire clause immediately
            if isinstance(result, SOLFReturnValue):
                if quantifier == '∃':
                    if success_seen < success_skip:
                        success_seen += 1
                        if clause:
                            clause.restore_state(per_item_state)
                        else:
                            self.variables = per_item_state
                        continue
                    self._exist_choice_offsets[choice_key] = success_seen + 1
                restore_iter_bindings()
                return result
            
            # Handle results based on quantifier
            if quantifier == '∃':  # There exists
                if result is not False:
                    if success_seen < success_skip:
                        success_seen += 1
                        if clause:
                            clause.restore_state(per_item_state)
                        else:
                            self.variables = per_item_state
                        continue

                    self._exist_choice_offsets[choice_key] = success_seen + 1
                    # Keep variable bindings for selected successful candidate
                    restore_iter_bindings()
                    return True
                if clause:
                    clause.restore_state(per_item_state)
                else:
                    self.variables = per_item_state
                # Continue to next item
            
            elif quantifier == '∀':  # For all
                if result is not False:
                    success_count += 1
                # Continue processing all items
        
        # Finalize based on quantifier
        if quantifier == '∀':
            # Succeeds if body succeeded for at least one element
            if success_count > 0:
                # Keep the variables from the last successful iteration
                restore_iter_bindings()
                return True
            else:
                # Failed for all elements, restore original state
                if clause:
                    clause.restore_state(iteration_start_state)
                else:
                    self.variables = iteration_start_state
                restore_iter_bindings()
                return False
        
        elif quantifier == '∃':
            # Failed to find any matching element, restore original state
            if choice_key is not None:
                self._exist_choice_offsets[choice_key] = 0
            if clause:
                clause.restore_state(iteration_start_state)
            else:
                self.variables = iteration_start_state
            restore_iter_bindings()
            return False
        
        restore_iter_bindings()
        return False
    
    def _execute_predicate_internal(self, predicate: Any, clause: Optional['Clause'] = None) -> Any:
        self._debug_before_eval(predicate, clause)
        result = self._execute_predicate_internal_impl(predicate, clause)
        self._debug_after_eval(predicate, result, clause)
        if isinstance(result, SOLFReturnValue):
            self._debug_on_return(predicate, result.value, clause)
        return result

    def _execute_predicate_internal_impl(self, predicate: Any, clause: Optional['Clause'] = None) -> Any:
        """
        Internal execution method for a single predicate.
        Handles tuple predicates and delegates chains to _execute_predicate_chain.
        
        Args:
            predicate: The predicate to execute (should be a tuple for atomic operations).
            clause: The clause instance containing variable bindings.
            
        Returns:
            The result of executing the predicate.
        """
        # Use clause variables if available, otherwise fall back to interpreter variables
        context = clause.variables if clause else self.variables
        
        # Handle None or empty predicate
        if predicate is None:
            return None
        
        # Handle atomic values (strings, numbers, booleans)
        if isinstance(predicate, (str, int, float, bool)):
            if isinstance(predicate, str):
                # Handle object attribute access (e.g., benjamin.age)
                if '.' in predicate and not predicate.startswith('_'):
                    parts = predicate.split('.', 1)
                    obj_name = parts[0]
                    attr_name = parts[1] if len(parts) > 1 else ''

                    obj_alias = context.get(obj_name, self.variables.get(obj_name))
                    if isinstance(obj_alias, str) and obj_alias in self.objects:
                        obj_name = obj_alias

                    # Runtime object-like values defined in Program mode:
                    # yeung ≔ {age: 72, getAge: ...}
                    runtime_obj = context.get(obj_name, self.variables.get(obj_name))
                    if isinstance(runtime_obj, dict) and attr_name:
                        if attr_name in runtime_obj:
                            return runtime_obj[attr_name]
                    
                    # Look up attribute in object with inheritance
                    if obj_name in self.objects:
                        result, _ = self._lookup_in_object(obj_name, attr_name)
                        return result if result is not None else predicate
                
                # Handle variable lookup
                if predicate.startswith('_'):
                    return context.get(predicate, predicate)

                # Optional plain-identifier lookup for runtime object method scopes.
                if context.get('__allow_plain_lookup__') and predicate in context:
                    return context.get(predicate)
                
                # Handle attribute lookup within object method (when 'this' is set)
                # If we're inside an object method and reference a non-variable identifier,
                # try to look it up as an attribute of 'this' object
                if 'this' in context:
                    this_obj = context['this']
                    result, _ = self._lookup_in_object(this_obj, predicate)
                    if result is not None:
                        return result
            return predicate
        
        # Handle lists (complex expressions / predicate chains)
        if isinstance(predicate, list):
            # Check if this is a simple binary operation in list format: [operand1, operator, operand2]
            if (len(predicate) == 3 and 
                isinstance(predicate[1], str) and 
                predicate[1] not in ['⋀', '⋁']):  # Not a logical connective
                
                # Convert to tuple format for operators
                operator = predicate[1]
                operand1 = predicate[0]
                operand2 = predicate[2]
                
                # Check if this is a known operator
                known_operators = ['@', '+', '-', '*', '/', '÷', '%', '=', '≠', '>', '<', '≥', '≤', 
                                 '≈', '∈', '∉', '⋂', '⋃', '∖', '⊆', '⊂', '⊄', '⥹', '⥻']
                
                if operator in known_operators:
                    # Convert to tuple and process
                    return self._evaluate_child((operator, [operand1, operand2]), clause, [0])
            
            # Check if this is an iteration expression
            # Format: [('∈', [('∀', ['_x']), '_numbers']), '⋀', body_predicates]
            if (len(predicate) >= 3 and predicate[1] == '⋀' and 
                isinstance(predicate[0], tuple) and len(predicate[0]) == 2 and
                predicate[0][0] == '∈' and isinstance(predicate[0][1], list) and
                len(predicate[0][1]) == 2 and isinstance(predicate[0][1][0], tuple) and
                predicate[0][1][0][0] in ['∀', '∃']):
                
                # Parse iteration structure
                quantifier_expr, logical_op, *body_parts = predicate
                _, quantifier_parts = quantifier_expr
                quantifier_tuple, container_var = quantifier_parts
                quantifier_op, var_list = quantifier_tuple
                
                variable = var_list[0] if isinstance(var_list, list) and len(var_list) == 1 else var_list
                
                # Evaluate collection
                collection = self._evaluate_child(container_var, clause, [0, 1, 1])
                
                # Reconstruct body predicates (everything after the quantifier and ⋀)
                if len(body_parts) == 1:
                    body_predicates = body_parts[0]
                else:
                    body_predicates = body_parts
                
                # Execute iteration
                return self._execute_iteration(quantifier_op, variable, collection, body_predicates, clause)
            
            # Check for standalone iteration without full iteration pattern
            # Format: [('∀', ['_x', '_numbers']), '⋀', body...]
            if (len(predicate) >= 3 and predicate[1] == '⋀' and
                isinstance(predicate[0], tuple) and len(predicate[0]) == 2 and
                predicate[0][0] in ['∀', '∃']):
                
                quantifier_expr = predicate[0]
                quantifier_op, operands = quantifier_expr
                
                if len(operands) == 2:
                    variable = operands[0]
                    collection_expr = operands[1]
                    
                    # Evaluate collection
                    collection = self._evaluate_child(collection_expr, clause, [0, 1, 1])
                    
                    # Body is everything after the quantifier and ⋀
                    body_predicates = predicate[2:] if len(predicate) > 3 else predicate[2]
                    
                    # Execute iteration
                    return self._execute_iteration(quantifier_op, variable, collection, body_predicates, clause)
            
            # Otherwise, this is a predicate chain - delegate to chain handler
            return self._execute_predicate_chain(predicate, clause)
        
        # Handle tuple expressions (atomic operations with operators)
        if isinstance(predicate, tuple) and len(predicate) == 2:
            operator, operands = predicate
            
            # Evaluate operands first (except for special cases)
            if operator not in ['≔', '≪', '∃', '⥹', '⥻']:  # Don't evaluate LHS for assignment-like operators
                if isinstance(operands, list):
                    evaluated_operands = []
                    for operand_idx, operand in enumerate(operands):
                        result = self._evaluate_child(operand, clause, [1, operand_idx])
                        
                        # Check if operand evaluation returned a return value
                        if isinstance(result, SOLFReturnValue):
                            return result
                        
                        evaluated_operands.append(result)
                else:
                    result = self._evaluate_child(operands, clause, [1])
                    
                    if isinstance(result, SOLFReturnValue):
                        return result
                    
                    evaluated_operands = [result]
            else:
                # For assignment operators, keep operands unevaluated initially
                evaluated_operands = operands if isinstance(operands, list) else [operands]
            
            # Handle return operator
            if operator == '↲':  # Return operator
                if len(operands) == 1:
                    operand = operands[0]
                    if isinstance(operand, tuple) and len(operand) == 2 and operand[0] == '𝒯':
                        # Tuple return
                        tuple_elements = []
                        for element_idx, element in enumerate(operand[1]):
                            tuple_elements.append(self._evaluate_child(element, clause, [1, 0, 1, element_idx]))
                        return SOLFReturnValue(tuple(tuple_elements))
                    else:
                        # Single value return - reuse the already-evaluated operand
                        # when available so side-effecting function calls are not
                        # executed twice under ↲(...).
                        evaluated = evaluated_operands[0] if evaluated_operands else self._evaluate_child(operand, clause, [1, 0])
                        return SOLFReturnValue(evaluated)
                else:
                    # Multiple operands - return as tuple
                    tuple_elements = list(evaluated_operands) if evaluated_operands else []
                    if not tuple_elements:
                        for operand_idx, operand in enumerate(operands):
                            tuple_elements.append(self._evaluate_child(operand, clause, [1, operand_idx]))
                    return SOLFReturnValue(tuple(tuple_elements))
            
            # Handle assignment operator
            elif operator == '≔':  # Assignment
                if len(operands) == 2:
                    var_name = operands[0]
                    value = self._evaluate_child(operands[1], clause, [1, 1])
                    
                    if isinstance(value, SOLFReturnValue):
                        return value
                    
                    # Handle element assignment (list/dict indexing): _container[index] ≔ value
                    if isinstance(var_name, tuple) and var_name[0] == '@' and len(var_name) >= 2:
                        operands_access = var_name[1]
                        if isinstance(operands_access, list) and len(operands_access) == 2:
                            container_expr = operands_access[0]
                            index_expr = operands_access[1]
                            
                            # Get the container and index
                            if isinstance(container_expr, str):
                                # container_expr is a variable name
                                container = self.variables.get(container_expr)
                                if clause:
                                    container = clause.variables.get(container_expr, container)
                                if container is None:
                                    container = context.get(container_expr)
                            else:
                                # Evaluate complex container expression
                                container = self._evaluate_child(container_expr, clause, [1, 0, 1, 0])
                            
                            # Evaluate the index
                            index = self._evaluate_child(index_expr, clause, [1, 0, 1, 1])
                            
                            # Handle string indices (for dict-like access)
                            if isinstance(index, str) and index.startswith('_'):
                                index = context.get(index, index)
                            
                            # Assign to the container
                            if container is not None:
                                try:
                                    if isinstance(container, dict):
                                        container[index] = value
                                    elif isinstance(container, list):
                                        if isinstance(index, int) and 0 <= index < len(container):
                                            container[index] = value
                                        else:
                                            return False
                                    else:
                                        return False
                                    
                                    # Update the container in all places it's stored
                                    if isinstance(container_expr, str):
                                        self.variables[container_expr] = container
                                        if clause:
                                            clause.variables[container_expr] = container
                                        context[container_expr] = container
                                    
                                    return value
                                except (KeyError, IndexError, TypeError):
                                    return False
                        return False
                    
                    # Handle tuple assignment
                    elif isinstance(var_name, tuple) and var_name[0] == '𝒯':
                        variable_names = var_name[1]
                        if isinstance(value, (tuple, list)) and len(variable_names) == len(value):
                            for i, var in enumerate(variable_names):
                                if isinstance(var, str):
                                    self.variables[var] = value[i]
                                    if clause:
                                        clause.variables[var] = value[i]
                                    context[var] = value[i]
                            return value
                        return False
                    
                    # Handle regular assignment
                    elif isinstance(var_name, str):
                        self.variables[var_name] = value
                        if clause:
                            clause.variables[var_name] = value
                        context[var_name] = value
                        return value
                    
                    return False
                return False
            
            # Handle unary operators
            elif operator == '∃':  # Exists (unary)
                # Special handling: if operand is an assignment with clause invocation,
                # stop backtracking on first successful assignment
                if len(operands) == 1:
                    operand_expr = operands[0]
                    
                    # Check if the operand is an assignment expression
                    if isinstance(operand_expr, tuple) and len(operand_expr) == 2:
                        op, op_operands = operand_expr
                        
                        # If it's an assignment, execute it with stop_on_first flag
                        if op == '≔' and len(op_operands) == 2:
                            # Set flag to stop on first successful clause match
                            old_stop_on_first = getattr(clause, 'stop_on_first', False) if clause else False
                            if clause:
                                clause.stop_on_first = True
                            else:
                                # Set it on a temporary attribute for the interpreter
                                self._stop_on_first = True
                            
                            try:
                                # Execute the assignment
                                result = self._evaluate_child(operand_expr, clause, [1, 0])
                                
                                # Return True if assignment succeeded, False otherwise
                                if isinstance(result, SOLFReturnValue):
                                    return True
                                return bool(result)
                            finally:
                                # Restore the flag
                                if clause:
                                    clause.stop_on_first = old_stop_on_first
                                else:
                                    self._stop_on_first = False
                    
                    # Default behavior: check if value exists
                    evaluated = self._evaluate_child(operand_expr, clause, [1, 0])
                    # Unresolved variable tokens should count as non-existent.
                    # Example: operand _child evaluates to literal "_child" when
                    # unbound in current scope.
                    if (
                        isinstance(operand_expr, str)
                        and operand_expr.startswith('_')
                        and evaluated == operand_expr
                    ):
                        return False
                    if evaluated is None:
                        return False
                    if isinstance(evaluated, (list, set, dict, tuple)):
                        return len(evaluated) > 0
                    return True
                return False

            # Handle set merge operators with directional target semantics
            elif operator in ['⥹', '⥻']:
                if len(operands) == 2:
                    # set1 ⥻ set2 -> add all elements from set2 into set1 (target is left)
                    # set1 ⥹ set2 -> add all elements from set1 into set2 (target is right)
                    if operator == '⥻':
                        target_expr, source_expr = operands[0], operands[1]
                    else:
                        source_expr, target_expr = operands[0], operands[1]

                    source_value = self._evaluate_child(source_expr, clause, [1, 1 if operator == '⥻' else 0])
                    if isinstance(source_value, SOLFReturnValue):
                        return source_value

                    def to_set(value):
                        if value is None:
                            return set()
                        if isinstance(value, set):
                            return set(value)
                        if isinstance(value, (list, tuple)):
                            return set(value)
                        if isinstance(value, dict):
                            return set(value.keys())
                        return {value}

                    # Target is a named variable
                    if isinstance(target_expr, str):
                        target_value = self.variables.get(target_expr)
                        if clause:
                            target_value = clause.variables.get(target_expr, target_value)
                        if target_value is None:
                            target_value = context.get(target_expr)

                        merged = to_set(target_value) | to_set(source_value)
                        self.variables[target_expr] = merged
                        if clause:
                            clause.variables[target_expr] = merged
                        context[target_expr] = merged
                        return merged

                    # Target is indexed assignment target: _container[index]
                    if isinstance(target_expr, tuple) and target_expr[0] == '@' and len(target_expr) >= 2:
                        target_operands = target_expr[1]
                        if isinstance(target_operands, list) and len(target_operands) == 2:
                            container_expr, index_expr = target_operands

                            if isinstance(container_expr, str):
                                container = self.variables.get(container_expr)
                                if clause:
                                    container = clause.variables.get(container_expr, container)
                                if container is None:
                                    container = context.get(container_expr)
                            else:
                                container = self._evaluate_child(container_expr, clause, [1, 0, 1, 0] if operator == '⥻' else [1, 1, 1, 0])

                            index = self._evaluate_child(index_expr, clause, [1, 0, 1, 1] if operator == '⥻' else [1, 1, 1, 1])
                            if isinstance(index, str) and index.startswith('_'):
                                index = context.get(index, index)

                            if container is None:
                                return False

                            try:
                                current_value = container[index]
                            except (KeyError, IndexError, TypeError):
                                return False

                            merged = to_set(current_value) | to_set(source_value)

                            try:
                                if isinstance(container, dict):
                                    container[index] = merged
                                elif isinstance(container, list):
                                    if isinstance(index, int) and 0 <= index < len(container):
                                        container[index] = merged
                                    else:
                                        return False
                                else:
                                    return False
                            except (KeyError, IndexError, TypeError):
                                return False

                            if isinstance(container_expr, str):
                                self.variables[container_expr] = container
                                if clause:
                                    clause.variables[container_expr] = container
                                context[container_expr] = container

                            return merged

                    # Fallback: pure set merge result without mutating storage
                    target_value = self._evaluate_child(target_expr, clause, [1, 0 if operator == '⥻' else 1])
                    return to_set(target_value) | to_set(source_value)

                return False
            
            elif operator == '∄':  # Not Exists (unary)
                if len(evaluated_operands) == 1:
                    operand = evaluated_operands[0]
                    # Negation of ∃
                    if operand is None:
                        return True
                    if isinstance(operand, (list, set, dict, tuple)):
                        return len(operand) == 0
                    return False
                return False
            
            elif operator == '¬':  # Logical negation
                if len(evaluated_operands) == 1:
                    return not bool(evaluated_operands[0])
                return False
            
            elif operator == '#':  # Length/cardinality
                if len(evaluated_operands) == 1:
                    operand = evaluated_operands[0]
                    if hasattr(operand, '__len__'):
                        return len(operand)
                return 0
            
            # Note: τ is now handled as a function call (trace/print)
            # See function call handling section below
            
            elif operator == 'ℰ':  # Return vars dict
                if clause:
                    return clause.variables.copy()
                else:
                    return self.variables.copy()
            
            # Handle arithmetic operators
            elif operator == '+':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands

                    # 1) numeric addition
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left + right

                    # 2) string concatenation (for τ tracing and mixed values)
                    if isinstance(left, str) or isinstance(right, str):
                        return f"{self._trace_text(left)}{self._trace_text(right)}"

                    # 3) collection union fallback (preserve previous behavior)
                    def to_set(value):
                        if isinstance(value, set):
                            return value
                        if isinstance(value, list):
                            return set(value)
                        if isinstance(value, tuple):
                            return set(value)
                        if isinstance(value, dict):
                            return set(value.keys())
                        return {value}

                    return to_set(left) | to_set(right)
                return 0
            
            elif operator == '-':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left - right
                return 0
            
            elif operator == '*':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left * right
                return 0
            
            elif operator == '/':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)) and right != 0:
                        return left / right
                return 0
            
            elif operator == '÷':  # Division symbol
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)) and right != 0:
                        return left / right
                return 0
            
            elif operator == '%':  # Modulo
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)) and right != 0:
                        return left % right
                return 0
            
            # Handle comparison operators
            elif operator == '=':
                if len(evaluated_operands) == 2:
                    return evaluated_operands[0] == evaluated_operands[1]
                return False
            
            elif operator == '≠':
                if len(evaluated_operands) == 2:
                    return evaluated_operands[0] != evaluated_operands[1]
                return False
            
            elif operator == '>':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left > right
                return False
            
            elif operator == '<':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left < right
                return False
            
            elif operator == '≥':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left >= right
                return False
            
            elif operator == '≤':
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                        return left <= right
                return False
            
            elif operator == '≈':  # Approximate equality (same elements, different order)
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    if isinstance(left, list) and isinstance(right, list):
                        return sorted(left) == sorted(right)
                    return left == right
                return False
            
            # Handle set operations
            elif operator == '∈':  # Element in set
                if len(evaluated_operands) == 2:
                    element, container = evaluated_operands
                    if isinstance(container, (set, list, tuple)):
                        return element in container
                    elif isinstance(container, dict):
                        return element in container.keys()
                return False
            
            elif operator == '∉':  # Element not in set
                if len(evaluated_operands) == 2:
                    element, container = evaluated_operands
                    if isinstance(container, (set, list, tuple)):
                        return element not in container
                    elif isinstance(container, dict):
                        return element not in container.keys()
                return False
            
            elif operator == '⋂':  # Set intersection
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    left_set = set(left) if not isinstance(left, set) else left
                    right_set = set(right) if not isinstance(right, set) else right
                    return left_set & right_set
                return set()
            
            elif operator == '⋃':  # Set union
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    left_set = set(left) if not isinstance(left, set) else left
                    right_set = set(right) if not isinstance(right, set) else right
                    return left_set | right_set
                return set()
            
            elif operator == '∖':  # Set difference
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    left_set = set(left) if not isinstance(left, set) else left
                    right_set = set(right) if not isinstance(right, set) else right
                    return left_set - right_set
                return set()
            
            elif operator == '⊆':  # Subset
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    left_set = set(left) if not isinstance(left, set) else left
                    right_set = set(right) if not isinstance(right, set) else right
                    return left_set <= right_set
                return False
            
            elif operator == '⊂':  # Proper subset
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    left_set = set(left) if not isinstance(left, set) else left
                    right_set = set(right) if not isinstance(right, set) else right
                    return left_set < right_set
                return False

            elif operator == '⊄':  # Not proper subset
                if len(evaluated_operands) == 2:
                    left, right = evaluated_operands
                    left_set = set(left) if not isinstance(left, set) else left
                    right_set = set(right) if not isinstance(right, set) else right
                    return not (left_set < right_set)
                return False
            
            # Handle data structure access
            elif operator == '@':  # Element access
                if len(operands) == 2:
                    # Keep container unevaluated for fact access
                    container_expr = operands[0]
                    index_expr = operands[1]
                    
                    # Evaluate index
                    index = self._evaluate_child(index_expr, clause, [1, 1])
                    
                    # Check if container is a fact name
                    if isinstance(container_expr, str) and container_expr in self.facts:
                        fact_data = self.facts[container_expr]
                        if isinstance(fact_data, dict):
                            # Resolve index if it's a variable
                            if isinstance(index, str) and index.startswith('_'):
                                index = context.get(index, index)
                            return fact_data.get(index)
                        elif isinstance(fact_data, (list, tuple)):
                            if isinstance(index, int) and 0 <= index < len(fact_data):
                                return fact_data[index]
                        return None

                    # Runtime named values in Program mode (e.g. age ≔ {...})
                    # should be indexable via age[key] even though they are not
                    # underscore-prefixed variables.
                    if isinstance(container_expr, str):
                        if container_expr in context:
                            container = context.get(container_expr)
                        elif container_expr in self.variables:
                            container = self.variables.get(container_expr)
                        else:
                            container = self._evaluate_child(container_expr, clause, [1, 0])
                    else:
                        # Otherwise evaluate container expression
                        container = self._evaluate_child(container_expr, clause, [1, 0])
                    
                    try:
                        if isinstance(container, dict):
                            return container[index]
                        elif isinstance(container, (list, tuple, str)):
                            if isinstance(index, int):
                                return container[index]
                        return None
                    except (KeyError, IndexError, TypeError):
                        return None
                return None
            
            # Handle data structure creation
            elif operator == 'ℒ':  # List creation
                return list(evaluated_operands)
            
            elif operator == '𝒮':  # Set creation
                return set(evaluated_operands)
            
            elif operator == '𝒯':  # Tuple creation
                return tuple(evaluated_operands)
            
            elif operator == 'ℳ':  # Dictionary creation
                result = {}
                # Preferred parser shape: [['k1', v1], ['k2', v2], ...]
                if all(isinstance(item, (list, tuple)) and len(item) == 2 for item in evaluated_operands):
                    for pair_idx, (key, value) in enumerate(evaluated_operands):
                        if isinstance(key, (tuple, list)):
                            key = self._evaluate_child(key, clause, [1, 0])
                        if isinstance(value, SOLFReturnValue):
                            raw_pair = None
                            if isinstance(operands, list) and pair_idx < len(operands):
                                raw_pair = operands[pair_idx]
                            if isinstance(raw_pair, (list, tuple)) and len(raw_pair) == 2:
                                value = raw_pair[1]
                            else:
                                value = value.value
                        if isinstance(value, (tuple, list)):
                            value = self._evaluate_child(value, clause, [1, 1])
                        if isinstance(value, SOLFReturnValue):
                            raw_pair = None
                            if isinstance(operands, list) and pair_idx < len(operands):
                                raw_pair = operands[pair_idx]
                            if isinstance(raw_pair, (list, tuple)) and len(raw_pair) == 2:
                                value = raw_pair[1]
                            else:
                                value = value.value
                        if isinstance(key, (list, dict, set)):
                            key = str(key)
                        result[key] = value
                    return result

                # Backward-compatible flat shape: [k1, v1, k2, v2, ...]
                for i in range(0, len(evaluated_operands), 2):
                    if i + 1 < len(evaluated_operands):
                        key = evaluated_operands[i]
                        value = evaluated_operands[i + 1]
                        if isinstance(key, (list, dict, set)):
                            key = str(key)
                        result[key] = value
                return result
            
            # Handle string literals
            elif operator == '"':  # String literal
                if len(evaluated_operands) == 1:
                    return str(evaluated_operands[0])
                return ""
            
            # Handle list append operator
            elif operator == '≪':  # Append to list
                if len(operands) == 2:
                    list_var = operands[0]
                    value = self._evaluate_child(operands[1], clause, [1, 1])
                    
                    if isinstance(value, SOLFReturnValue):
                        return value
                    
                    if isinstance(list_var, str):
                        if list_var not in self.variables:
                            self.variables[list_var] = []
                        if not isinstance(self.variables[list_var], list):
                            self.variables[list_var] = []
                        self.variables[list_var].append(value)
                        if clause:
                            if list_var not in clause.variables:
                                clause.variables[list_var] = []
                            if not isinstance(clause.variables[list_var], list):
                                clause.variables[list_var] = []
                            clause.variables[list_var].append(value)
                        context[list_var] = self.variables[list_var]
                        return True
                    return False
                return False
            
            # Handle function calls (facts, clauses, predefined functions)
            # Format: (function_name, [arg1, arg2, ...])
            else:
                # Check if this is a function call
                if isinstance(operator, str):
                    # 4. Handle object attribute/method access (e.g., benjamin.age or benjamin.getAddress())
                    if '.' in operator:
                        parts = operator.split('.', 1)  # Split only on first dot
                        obj_name = parts[0]
                        attr_or_method = parts[1] if len(parts) > 1 else ''

                        obj_alias = context.get(obj_name, self.variables.get(obj_name))
                        if isinstance(obj_alias, str) and obj_alias in self.objects:
                            obj_name = obj_alias

                        # Runtime object-like dict methods/attributes from Program mode.
                        runtime_obj = context.get(obj_name, self.variables.get(obj_name))
                        if isinstance(runtime_obj, dict) and attr_or_method in runtime_obj:
                            attr_value = runtime_obj[attr_or_method]

                            # If attribute value is an inline predicate/body AST, execute it
                            # as a method body in a temporary clause scope seeded with object fields.
                            if isinstance(attr_value, (tuple, list)):
                                method_clause = Clause(
                                    name=f"{obj_name}.{attr_or_method}",
                                    predicates=attr_value,
                                    arguments=[],
                                )
                                # Start from caller/interpreter vars, then overlay object fields.
                                method_clause.variables.update(self.variables)
                                if clause:
                                    method_clause.variables.update(clause.variables)
                                method_clause.variables.update(runtime_obj)
                                method_clause.variables['this'] = obj_name
                                method_clause.variables['self'] = obj_name
                                method_clause.variables['__allow_plain_lookup__'] = True

                                result = self._execute_predicate_internal(attr_value, method_clause)
                                return result.value if isinstance(result, SOLFReturnValue) else result

                            # For plain attributes, allow property-style access via zero-arg call.
                            if not evaluated_operands:
                                return attr_value
                        
                        # Handle variable object reference (e.g., _obj.attribute)
                        if obj_name.startswith('_'):
                            obj_value = context.get(obj_name)
                            if isinstance(obj_value, dict) and attr_or_method in obj_value:
                                return obj_value[attr_or_method]
                            # If obj_value is a string (object name), use it for lookup
                            if isinstance(obj_value, str):
                                obj_name = obj_value
                        
                        # Try to find object in self.objects
                        if obj_name in self.objects:
                            # Look up attribute/method with inheritance
                            result, owner = self._lookup_in_object(obj_name, attr_or_method)
                            
                            if result is not None:
                                # If it's a clause (method), invoke it
                                if isinstance(result, str) and result.startswith('⦃'):
                                    # Parse and cache the method as a clause
                                    temp_name = f"{owner}.{attr_or_method}"
                                    
                                    # Check if already parsed and cached
                                    if temp_name not in self.clauses:
                                        parsed = self._parse_clause_definition(attr_or_method, result)
                                        self.clauses[temp_name] = [parsed]
                                    
                                    # Invoke the method with the object context
                                    # Create a special clause that has access to 'this'
                                    if clause:
                                        # Store current object reference in clause variables
                                        old_this = clause.variables.get('this')
                                        old_self = clause.variables.get('self')
                                        clause.variables['this'] = obj_name
                                        clause.variables['self'] = obj_name
                                    else:
                                        old_this = self.variables.get('this')
                                        old_self = self.variables.get('self')
                                        self.variables['this'] = obj_name
                                        self.variables['self'] = obj_name
                                    
                                    try:
                                        result = self._invoke_clause(
                                            temp_name,
                                            evaluated_operands if evaluated_operands else [],
                                            clause,
                                            operands if isinstance(operands, list) else [operands],
                                        )
                                        return result
                                    finally:
                                        # Restore previous 'this' value
                                        if clause:
                                            if old_this is None:
                                                clause.variables.pop('this', None)
                                            else:
                                                clause.variables['this'] = old_this
                                            if old_self is None:
                                                clause.variables.pop('self', None)
                                            else:
                                                clause.variables['self'] = old_self
                                        else:
                                            if old_this is None:
                                                self.variables.pop('this', None)
                                            else:
                                                self.variables['this'] = old_this
                                            if old_self is None:
                                                self.variables.pop('self', None)
                                            else:
                                                self.variables['self'] = old_self
                                elif isinstance(result, (tuple, list)):
                                    method_clause = Clause(
                                        name=f"{owner}.{attr_or_method}",
                                        predicates=result,
                                        arguments=[],
                                    )
                                    method_clause.variables.update(self.variables)
                                    if clause:
                                        method_clause.variables.update(clause.variables)
                                    method_clause.variables['this'] = obj_name
                                    method_clause.variables['self'] = obj_name
                                    method_clause.variables['__allow_plain_lookup__'] = True

                                    method_result = self._execute_predicate_internal(result, method_clause)
                                    return method_result.value if isinstance(method_result, SOLFReturnValue) else method_result
                                else:
                                    # It's an attribute, return its value
                                    return result
                            
                            # Not found in object or ancestors
                            return None
                    
                    # Lookup order: facts, clauses, predefined functions
                    
                    # 1. Check for fact lookup
                    if operator in self.facts:
                        return self._invoke_fact(operator, evaluated_operands if evaluated_operands else None)
                    
                    # 2. Check for clause invocation
                    # If we're inside an object method (this is set), check object clauses first
                    if 'this' in context:
                        this_obj = context['this']
                        # Look up method in current object and ancestors
                        method_def, owner = self._lookup_in_object(this_obj, operator)
                        if method_def is not None and isinstance(method_def, str) and method_def.startswith('⦃'):
                            # Found method in object/ancestor, invoke it
                            temp_name = f"{owner}.{operator}"
                            if temp_name not in self.clauses:
                                parsed = self._parse_clause_definition(operator, method_def)
                                self.clauses[temp_name] = [parsed]
                            return self._invoke_clause(
                                temp_name,
                                evaluated_operands if evaluated_operands else [],
                                clause,
                                operands if isinstance(operands, list) else [operands],
                            )
                        if isinstance(method_def, (tuple, list)):
                            method_clause = Clause(
                                name=f"{owner}.{operator}",
                                predicates=method_def,
                                arguments=[],
                            )
                            method_clause.variables.update(self.variables)
                            if clause:
                                method_clause.variables.update(clause.variables)
                            method_clause.variables['this'] = this_obj
                            method_clause.variables['self'] = this_obj
                            method_clause.variables['__allow_plain_lookup__'] = True

                            method_result = self._execute_predicate_internal(method_def, method_clause)
                            return method_result.value if isinstance(method_result, SOLFReturnValue) else method_result
                    
                    # Check global clauses
                    if operator in self.clauses:
                        return self._invoke_clause(
                            operator,
                            evaluated_operands if evaluated_operands else [],
                            clause,
                            operands if isinstance(operands, list) else [operands],
                        )
                    
                    # 3. Check for predefined functions
                    if operator in self.predefined_functions:
                        func = self.predefined_functions[operator]
                        return func(*evaluated_operands) if evaluated_operands else func()

                    # 3b. Check solf_function.py for a Python function with the same name
                    py_result = self._try_python_function(operator, evaluated_operands if evaluated_operands else [])
                    if py_result is not None:
                        return py_result

                    # 4. Treat runtime variables as queryable fact-like values.
                    # This lets UI-created statements such as:
                    #   man ≔ [Yeung, Benjamin, Damian, Severin]
                    # be queried with:
                    #   man(_)
                    # even though they were not loaded through load_json_definition().
                    if operator in context or operator in self.variables:
                        runtime_fact_data = context.get(operator, self.variables.get(operator))
                        return self._query_fact_data(runtime_fact_data, evaluated_operands if evaluated_operands else None)
                    
                    # 5. Handle dynamic invocation (_n ≔ human) ⋀ (_r ≔ _n(Kayla))
                    if operator.startswith('_'):
                        # Variable contains clause name
                        clause_name = context.get(operator, operator)
                        if isinstance(clause_name, str):
                            # Check if it's a fact, clause, or function
                            if clause_name in self.facts:
                                return self._invoke_fact(clause_name, evaluated_operands if evaluated_operands else None)
                            if clause_name in self.clauses:
                                return self._invoke_clause(
                                    clause_name,
                                    evaluated_operands if evaluated_operands else [],
                                    clause,
                                    operands if isinstance(operands, list) else [operands],
                                )
                            if clause_name in self.predefined_functions:
                                func = self.predefined_functions[clause_name]
                                return func(*evaluated_operands) if evaluated_operands else func()
        
        # Default case - return predicate as-is
        return predicate
    
    def load_json_definition(self, json_def):
        """
        Load a JSON definition of objects, facts, and clauses.
        
        Args:
            json_def: A dictionary with "objects", "facts", and "clauses" keys.
        """
        if 'objects' in json_def:
            self.objects.update(json_def['objects'])
        
        if 'facts' in json_def:
            # Parse facts - they can be various formats
            for fact_name, fact_data in json_def['facts'].items():
                self.facts[fact_name] = self._parse_fact(fact_data)
        
        if 'clauses' in json_def:
            # Parse clauses - format: ⦃args | predicates⦄
            for clause_name, clause_def in json_def['clauses'].items():
                # A clause can be a string, a list of strings (multi-line), or a list of definitions (multiple clauses)
                if isinstance(clause_def, list):
                    # Check if this is a list of separate clause definitions or multi-line definition
                    # If all items contain ⦃, treat as separate definitions
                    if all(isinstance(item, str) and '⦃' in item for item in clause_def):
                        # Multiple separate clause definitions for same name
                        for single_def in clause_def:
                            clause_str = single_def
                            parsed_clause = self._parse_clause_definition(clause_name, clause_str)
                            
                            if clause_name not in self.clauses:
                                self.clauses[clause_name] = []
                            self.clauses[clause_name].append(parsed_clause)
                    else:
                        # Multi-line definition for single clause
                        clause_str = ''.join(clause_def)
                        parsed_clause = self._parse_clause_definition(clause_name, clause_str)
                        
                        if clause_name not in self.clauses:
                            self.clauses[clause_name] = []
                        self.clauses[clause_name].append(parsed_clause)
                else:
                    # Single clause definition as string
                    clause_str = clause_def
                    parsed_clause = self._parse_clause_definition(clause_name, clause_str)
                    
                    if clause_name not in self.clauses:
                        self.clauses[clause_name] = []
                    self.clauses[clause_name].append(parsed_clause)

    def _extract_inline_script_clauses(self, script: str):
        """Extract inline clause definitions from Program-mode script text.

        Supported form:
            name(_a, _b) ⦃ ... ⦄

        Returns:
            (remaining_script, clauses_dict)
        """
        if not script or '⦃' not in script or '⦄' not in script:
            return script, {}

        header_re = re.compile(r'([A-Za-z_][A-Za-z0-9_\.]*)\s*\(([^)]*)\)\s*⦃', re.DOTALL)

        idx = 0
        chunks: List[str] = []
        clauses: Dict[str, Any] = {}

        while idx < len(script):
            match = header_re.search(script, idx)
            if not match:
                chunks.append(script[idx:])
                break

            start = match.start()
            open_brace_idx = match.end() - 1

            depth = 0
            close_brace_idx = -1
            for i in range(open_brace_idx, len(script)):
                ch = script[i]
                if ch == '⦃':
                    depth += 1
                elif ch == '⦄':
                    depth -= 1
                    if depth == 0:
                        close_brace_idx = i
                        break

            if close_brace_idx == -1:
                chunks.append(script[idx:])
                break

            chunks.append(script[idx:start])

            name = match.group(1).strip()
            args = match.group(2).strip()
            body = script[open_brace_idx + 1:close_brace_idx].strip()

            clause_text = f"⦃{args} | {body}⦄" if args else f"⦃{body}⦄"

            existing = clauses.get(name)
            if existing is None:
                clauses[name] = clause_text
            elif isinstance(existing, list):
                existing.append(clause_text)
            else:
                clauses[name] = [existing, clause_text]

            idx = close_brace_idx + 1

        return ''.join(chunks), clauses

    def _normalize_program_script(self, script: str) -> str:
        """Normalize free-form Program mode text into a parser-friendly chain."""
        if not script:
            return ''

        if '⋀' in script or '⋁' in script:
            return script

        parts: List[str] = []
        current: List[str] = []
        paren = 0
        bracket = 0
        brace = 0

        for ch in script:
            if ch == '(':
                paren += 1
            elif ch == ')':
                paren = max(paren - 1, 0)
            elif ch == '[':
                bracket += 1
            elif ch == ']':
                bracket = max(bracket - 1, 0)
            elif ch == '{':
                brace += 1
            elif ch == '}':
                brace = max(brace - 1, 0)

            if ch == '\n' and paren == 0 and bracket == 0 and brace == 0:
                statement = ''.join(current).strip()
                if statement:
                    parts.append(statement)
                current = []
                continue

            current.append(ch)

        tail = ''.join(current).strip()
        if tail:
            parts.append(tail)

        if not parts:
            return ''
        if len(parts) == 1:
            return parts[0]
        return ' ⋀ '.join(parts)

    def _normalize_object_ancestors(self, raw_ancestors):
        """Normalize extends/extending payload to a clean ancestor-name list."""
        if raw_ancestors is None:
            return []
        if isinstance(raw_ancestors, str):
            names = [raw_ancestors]
        elif isinstance(raw_ancestors, (list, tuple, set)):
            names = list(raw_ancestors)
        else:
            return []

        normalized = []
        for name in names:
            if isinstance(name, str) and name and name not in normalized:
                normalized.append(name)
        return normalized

    def _normalize_program_object_definition(self, name: str, value: Any):
        """Convert Program-mode objectType/extending dicts into object metadata."""
        if not isinstance(name, str) or not isinstance(value, dict):
            return None

        object_type = value.get('objectType')
        extends_raw = value.get('extends', value.get('extending'))

        # Not an object/class declaration candidate.
        if object_type is None and extends_raw is None:
            return None

        normalized = copy.deepcopy(value)
        normalized.pop('extending', None)

        ancestors = self._normalize_object_ancestors(extends_raw)

        # objectType: class -> class definition named by assignment target.
        if isinstance(object_type, str) and object_type == 'class':
            if ancestors:
                normalized['extends'] = ancestors if len(ancestors) > 1 else ancestors[0]
            else:
                normalized.pop('extends', None)
            return normalized

        # objectType: <ClassName> -> runtime instance inheriting from className.
        if isinstance(object_type, str) and object_type:
            if object_type not in ancestors:
                ancestors.insert(0, object_type)

        if ancestors:
            normalized['extends'] = ancestors if len(ancestors) > 1 else ancestors[0]
        else:
            normalized.pop('extends', None)

        return normalized

    def load_program_script(self, program_script: str, clear_existing: bool = False) -> Dict[str, int]:
        """Load Program-mode definitions text into interpreter clauses/facts.

        This accepts script text as used in the Program Definitions editor:
        - clause declarations: name(args) ⦃ ... ⦄
        - assignment definitions: fact ≔ value

        Args:
            program_script: Raw Program-mode script text.
            clear_existing: If True, clear existing objects/facts/clauses/variables first.

        Returns:
            Summary dict with counts of loaded definitions.
        """
        if clear_existing:
            self.objects.clear()
            self.facts.clear()
            self.clauses.clear()
            self.variables.clear()

        script = program_script or ''
        remaining_script, clauses = self._extract_inline_script_clauses(script)

        if clauses:
            self.load_json_definition({'clauses': clauses})

        normalized = self._normalize_program_script(remaining_script).strip()
        if normalized:
            if self.parser is None:
                raise RuntimeError('Parser is not set. Call set_parser(...) before load_program_script().')

            parsed = self.parser.parse(normalized)
            ast, _ = self._extract_ast_and_meta(parsed)
            self.execute_predicate(ast)

        # Promote Program-mode object declarations to runtime objects.
        objects_loaded = 0
        for name, value in self.variables.items():
            normalized_object = self._normalize_program_object_definition(name, value)
            if normalized_object is None:
                continue
            self.objects[name] = normalized_object
            objects_loaded += 1

        # Promote top-level runtime assignments to facts for explicit fact lookups.
        for name, value in self.variables.items():
            if isinstance(name, str) and not name.startswith('_'):
                self.facts[name] = copy.deepcopy(value)

        clause_count = 0
        for val in clauses.values():
            clause_count += len(val) if isinstance(val, list) else 1

        fact_count = sum(1 for k in self.variables.keys() if isinstance(k, str) and not k.startswith('_'))

        return {
            'clauses_loaded': clause_count,
            'facts_loaded': fact_count,
            'objects_loaded': objects_loaded,
        }

    def load_program_file(self, file_path: str, clear_existing: bool = False) -> Dict[str, int]:
        """Load Program-mode definitions from a text file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f'Program file not found: {file_path}')
        with open(file_path, 'r', encoding='utf-8') as f:
            script = f.read()
        return self.load_program_script(script, clear_existing=clear_existing)
    
    def _parse_fact(self, fact_data):
        """
        Parse a fact from JSON data.
        
        Facts can be:
        - Simple string: "Yeung, Benjamin, Damian"
        - List of strings/tuples
        - Dictionary with nested data
        """
        if isinstance(fact_data, str):
            # Parse comma-separated atoms
            return [item.strip() for item in fact_data.split(',') if item.strip()]
        elif isinstance(fact_data, list):
            return fact_data
        elif isinstance(fact_data, dict):
            # Return as-is for indexed access
            return fact_data
        else:
            return fact_data
    
    def _parse_clause_definition(self, clause_name, clause_str):
        """
        Parse a clause definition from ⦃args | body⦄ or ⦃body⦄.
        Tries body parse first, then full-clause parse as fallback.
        """
        pattern_with_pipe = r'⦃\s*([^|]*?)\s*\|\s*(.*?)\s*⦄'
        match = re.search(pattern_with_pipe, clause_str, re.DOTALL)

        if match:
            args_str = match.group(1).strip()
            body_str = match.group(2).strip()
        else:
            pattern_no_pipe = r'⦃\s*(.*?)\s*⦄'
            match = re.search(pattern_no_pipe, clause_str, re.DOTALL)
            if match:
                args_str = ""
                body_str = match.group(1).strip()
            else:
                args_str = ""
                body_str = clause_str.strip()

        # Parse args
        args = []
        if args_str:
            for arg in [a.strip() for a in args_str.split(',') if a.strip()]:
                try:
                    if '.' not in arg:
                        args.append(int(arg))
                    else:
                        args.append(float(arg))
                except ValueError:
                    args.append(arg)

        parsed_body = None
        parsed_meta = None
        if self.parser is not None:
            # 1) Parse body only
            if body_str:
                try:
                    parsed_body = self.parser.parse(body_str)
                    parsed_body, parsed_meta = self._extract_ast_and_meta(parsed_body)
                except Exception as e:
                    print(f"[LOAD] clause={clause_name} body_parse_error={e}", flush=True)

            # 2) Fallback: parse full clause and extract body
            if parsed_body is None:
                try:
                    parsed_full = self.parser.parse(clause_str)
                    parsed_full, parsed_full_meta = self._extract_ast_and_meta(parsed_full)
                    parsed_body = self._extract_clause_body_from_parsed(parsed_full)
                    parsed_meta = parsed_full_meta
                except Exception as e:
                    print(f"[LOAD] clause={clause_name} full_parse_error={e}", flush=True)
        else:
            print(f"[LOAD] clause={clause_name} parser=None (call set_parser before load_json_definition)", flush=True)

        print(
            f"[LOAD] clause={clause_name} args={args} parsed_body={'OK' if parsed_body is not None else 'NONE'}",
            flush=True
        )

        return {
            'name': clause_name,
            'args': args,
            'body': body_str,
            'parsed_body': parsed_body,
            'parsed_meta': parsed_meta,
        }

    def _trace_text(self, value):
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            return "none"
        if isinstance(value, set):
            if not value:
                return "{}"
            return "{" + ", ".join(sorted(self._trace_text(v) for v in value)) + "}"
        if isinstance(value, list):
            return "[" + ", ".join(self._trace_text(v) for v in value) + "]"
        if isinstance(value, tuple):
            return "(" + ", ".join(self._trace_text(v) for v in value) + ")"
        if isinstance(value, dict):
            if not value:
                return "{}"
            return "{" + ", ".join(f"{self._trace_text(k)}: {self._trace_text(v)}" for k, v in value.items()) + "}"
        return str(value)    
    
    def _builtin_trace(self, *args):
        for arg in args:
            text = self._trace_text(arg)
            self.trace_messages.append(text)
            print(text)
        return True

    def get_trace_messages(self):
        return list(self.trace_messages)
    
    def _builtin_resource(self, resource_name):
        """ℛ(resource_name) - Access named resource."""
        # Placeholder for resource management
        return f"<Resource: {resource_name}>"
    
    def _builtin_text_lookup(self, name_atom):
        """⊤(name) - Text lookup for internationalization."""
        # Placeholder for i18n
        return f"<Text: {name_atom}>"
    
    def _builtin_sort(self, lst):
        """sort(list) - Sort a list."""
        if isinstance(lst, list):
            try:
                return sorted(lst)
            except TypeError:
                return lst
        return lst

    # ---------------------------------------------------------------------------
    # The methods below (_is_safe_bank_state, _builtin_river_crossing_solver)
    # were specific to the farmer/wolf/goat/cabbage puzzle and have been
    # commented out.  The interpreter should remain generic and must not
    # hard-code logic for individual clauses.  If a generic "built-in solver"
    # hook is needed in the future, it should accept an arbitrary callable
    # registered by the caller rather than naming a specific clause.
    # ---------------------------------------------------------------------------

    # def _is_safe_bank_state(self, side: Set[Any]) -> bool:
    #     """A bank is unsafe if wolf+goat or goat+cabbage are together without farmer."""
    #     if 'farmer' in side:
    #         return True
    #     if 'wolf' in side and 'goat' in side:
    #         return False
    #     if 'goat' in side and 'cabbage' in side:
    #         return False
    #     return True

    # def _builtin_river_crossing_solver(self, bank, no=0, crossing=None):
    #     """Deterministic solver for the classic farmer/wolf/goat/cabbage puzzle."""
    #     try:
    #         if not isinstance(bank, (list, tuple)) or len(bank) != 2:
    #             return False
    #
    #         side0 = set(bank[0]) if isinstance(bank[0], (set, list, tuple)) else set()
    #         side1 = set(bank[1]) if isinstance(bank[1], (set, list, tuple)) else set()
    #         farmer_side = int(no) if isinstance(no, int) else 0
    #
    #         # Build universe from provided sides and ensure core atoms exist.
    #         universe = set(side0) | set(side1)
    #         universe.update({'farmer', 'wolf', 'goat', 'cabbage'})
    #
    #         start = (frozenset(side0), frozenset(side1), farmer_side)
    #         queue = deque([start])
    #         visited = {start}
    #
    #         while queue:
    #             s0, s1, current_side = queue.popleft()
    #             curr0, curr1 = set(s0), set(s1)
    #
    #             if len(curr0) == 0:
    #                 if isinstance(bank, list) and len(bank) == 2:
    #                     bank[0] = set(curr0)
    #                     bank[1] = set(curr1)
    #                 return [curr0, curr1]
    #
    #             from_side = curr0 if current_side == 0 else curr1
    #             to_side = curr1 if current_side == 0 else curr0
    #
    #             if 'farmer' not in from_side:
    #                 continue
    #
    #             # Farmer alone, or farmer with one passenger.
    #             move_options = [None, 'wolf', 'goat', 'cabbage']
    #             for passenger in move_options:
    #                 move = {'farmer'} if passenger is None else {'farmer', passenger}
    #                 if not move.issubset(from_side):
    #                     continue
    #
    #                 new_from = set(from_side) - move
    #                 new_to = set(to_side) | move
    #
    #                 if current_side == 0:
    #                     next0, next1 = new_from, new_to
    #                 else:
    #                     next0, next1 = new_to, new_from
    #
    #                 if not self._is_safe_bank_state(next0):
    #                     continue
    #                 if not self._is_safe_bank_state(next1):
    #                     continue
    #
    #                 # Keep atoms inside the known universe.
    #                 next0 = set(x for x in next0 if x in universe)
    #                 next1 = set(x for x in next1 if x in universe)
    #
    #                 next_state = (frozenset(next0), frozenset(next1), 1 - current_side)
    #                 if next_state in visited:
    #                     continue
    #                 visited.add(next_state)
    #                 queue.append(next_state)
    #
    #         return False
    #     except Exception:
    #         return False
    
    def _lookup_in_object(self, obj_name, attribute_or_method, visited=None):
        """
        Look up an attribute or method in an object, following the inheritance chain.
        
        Resolution order:
        1. Check the object itself
        2. Check ancestors (recursive)
        3. Return None if not found
        
        Args:
            obj_name: Name of the object
            attribute_or_method: Name of the attribute/method to look up
            visited: Set of already visited objects (to prevent infinite loops)
            
        Returns:
            Tuple of (value, owner_obj_name) if found, else (None, None)
        """
        if visited is None:
            visited = set()
        
        # Prevent infinite loops in inheritance chain
        if obj_name in visited:
            return (None, None)
        visited.add(obj_name)
        
        # Check if object exists
        if obj_name not in self.objects:
            return (None, None)
        
        obj = self.objects[obj_name]
        if not isinstance(obj, dict):
            return (None, None)
        
        # 1. Check in the object itself
        if attribute_or_method in obj:
            return (obj[attribute_or_method], obj_name)
        
        # 2. Check in ancestors (supports 'extends' and legacy 'extending').
        ancestors = obj.get('extends', obj.get('extending'))
        if ancestors is not None:
            # Handle both single ancestor (string) and multiple ancestors (list)
            if isinstance(ancestors, str):
                ancestors = [ancestors]
            elif isinstance(ancestors, (tuple, set)):
                ancestors = list(ancestors)
            elif not isinstance(ancestors, list):
                ancestors = []
            
            # Try each ancestor in order
            for ancestor_name in ancestors:
                result, owner = self._lookup_in_object(ancestor_name, attribute_or_method, visited)
                if result is not None:
                    return (result, owner)
        
        # Not found
        return (None, None)
    
    def _match_arguments(self, clause_args, call_args, caller_clause=None, call_arg_refs=None):
        """
        Match call arguments to clause definition arguments.
        
        Args:
            clause_args: List of argument names from clause definition
            call_args: List of argument values from function call
            caller_clause: Optional caller clause for variable overwriting
            call_arg_refs: Optional list of original (unevaluated) call operands
            
        Returns:
            (matched, bindings) tuple where:
            - matched: Boolean indicating if arguments match
            - bindings: Dict of variable bindings for the new clause
            - overwrite_vars: List of variable names that should overwrite caller vars
        """
        bindings = {}
        overwrite_vars = []
        
        # Handle wildcard * - maps calling scope variables to clause arguments
        if len(call_args) == 1 and call_args[0] == '*':
            # Map all clause arguments from caller's variables
            caller_vars = caller_clause.variables if caller_clause else self.variables
            for arg in clause_args:
                if isinstance(arg, str) and arg.startswith('_'):
                    if arg in caller_vars:
                        bindings[arg] = caller_vars[arg]
                        overwrite_vars.append(arg)
            return (True, bindings, overwrite_vars)
        
        # Handle ellipsis ... - multiple unspecified trailing arguments
        if '...' in call_args:
            ellipsis_idx = call_args.index('...')
            # Match up to ellipsis
            for i in range(ellipsis_idx):
                if i >= len(clause_args):
                    return (False, {}, [])
                arg_name = clause_args[i]
                arg_value = call_args[i]
                
                # Resolve value if it's a variable
                if isinstance(arg_value, str) and arg_value.startswith('_'):
                    caller_vars = caller_clause.variables if caller_clause else self.variables
                    arg_value = caller_vars.get(arg_value, arg_value)
                    # Check if this variable should overwrite caller
                    if arg_value == arg_name:
                        overwrite_vars.append(arg_name)
                
                bindings[arg_name] = arg_value
            return (True, bindings, overwrite_vars)
        
        # Regular matching by position
        if len(call_args) != len(clause_args):
            return (False, {}, [])
        
        for i, (clause_arg, call_arg) in enumerate(zip(clause_args, call_args)):
            # Handle underscore _ - matches anything, no binding
            if call_arg == '_':
                continue
            
            # Handle atom matching - clause arg is atom (not a variable), call arg must match exactly
            # Atoms can be strings (not starting with _), numbers, or other literals
            is_clause_atom = not (isinstance(clause_arg, str) and clause_arg.startswith('_'))
            
            if is_clause_atom:
                # Clause arg is an atom - must match exactly
                if call_arg != clause_arg:
                    # Resolve call_arg if it's a variable
                    if isinstance(call_arg, str) and call_arg.startswith('_'):
                        caller_vars = caller_clause.variables if caller_clause else self.variables
                        resolved_value = caller_vars.get(call_arg, call_arg)
                        if resolved_value != clause_arg:
                            return (False, {}, [])
                    else:
                        return (False, {}, [])
                # No binding for atoms
                continue
            
            # Handle variable arguments
            if isinstance(clause_arg, str) and clause_arg.startswith('_'):
                # Resolve call_arg if it's a variable
                arg_value = call_arg

                # Preserve caller variable write-back when the original call operand
                # was the same variable name (arguments may already be evaluated).
                if call_arg_refs and i < len(call_arg_refs):
                    call_arg_ref = call_arg_refs[i]
                    if isinstance(call_arg_ref, str) and call_arg_ref == clause_arg:
                        overwrite_vars.append(clause_arg)

                if isinstance(call_arg, str) and call_arg.startswith('_'):
                    caller_vars = caller_clause.variables if caller_clause else self.variables
                    arg_value = caller_vars.get(call_arg, call_arg)
                    # Check if variable names match - if so, overwrite caller
                    if call_arg == clause_arg:
                        overwrite_vars.append(clause_arg)
                
                bindings[clause_arg] = arg_value
        
        return (True, bindings, overwrite_vars)

    def _ensure_clause_debug_metadata(self, clause_def):
        """Best-effort fetch of parser metadata for a clause body when debugging."""
        if clause_def.get('parsed_meta') is not None:
            return clause_def.get('parsed_meta')
        if self.parser is None:
            return None

        body_str = clause_def.get('body')
        if not body_str:
            return None

        try:
            parsed = self.parser.parse(body_str, meta=True)
        except TypeError:
            return None
        except Exception:
            return None

        parsed_body, parsed_meta = self._extract_ast_and_meta(parsed)
        if parsed_meta is not None:
            clause_def['parsed_meta'] = parsed_meta
        if clause_def.get('parsed_body') is None and parsed_body is not None:
            clause_def['parsed_body'] = parsed_body
        return clause_def.get('parsed_meta')
    
    def _invoke_clause(self, clause_name, call_args, caller_clause=None, call_arg_refs=None):
        """
        Invoke a clause with the given arguments.
        Implements Prolog-style backtracking - tries all matching clauses.
        
        Important: ↲ (return) stops execution within a predicate chain but does NOT
        act as a "cut" to prevent backtracking. This is different from Prolog's cut.
        The last successful clause result wins.
        
        Args:
            clause_name: Name of the clause to invoke
            call_args: List of arguments for the call
            caller_clause: Optional caller clause for variable resolution
            
        Returns:
            Result of last successful clause execution, or False if all fail
        """
        # if self.use_builtin_river_crossing_solver and clause_name == 'river_crossing':
        #     bank_arg = call_args[0] if len(call_args) > 0 else []
        #     no_arg = call_args[1] if len(call_args) > 1 else 0
        #     crossing_arg = call_args[2] if len(call_args) > 2 else set()
        #     solved = self._builtin_river_crossing_solver(bank_arg, no_arg, crossing_arg)
        #     if solved is not False:
        #         return solved

        is_root_call = len(self._clause_call_stack) == 0
        if is_root_call:
            # Keep memoization scoped to a single top-level invoke tree.
            self._failed_clause_signatures.clear()
            self._successful_clause_results.clear()
            self._exist_choice_offsets.clear()

        call_signature = self._build_call_signature(clause_name, call_args)
        current_depth = len(self._clause_call_stack) + 1
        same_call_count = self._clause_call_counts.get(call_signature, 0) + 1
        can_use_success_cache = False

        if call_signature in self._clause_call_stack:
            clauses_for_cycle = self.clauses.get(clause_name, []) if clause_name in self.clauses else []
            if clauses_for_cycle and not isinstance(clauses_for_cycle, list):
                clauses_for_cycle = [clauses_for_cycle]
            can_use_cycle_guard = bool(clauses_for_cycle) and self._clause_defs_are_pure(clauses_for_cycle)
            if can_use_cycle_guard:
                self._internal_log(f"[FAIL] clause={clause_name} reason=cycle_detected")
                return False

        if call_signature in self._failed_clause_signatures:
            clauses_for_memo = self.clauses.get(clause_name, []) if clause_name in self.clauses else []
            if clauses_for_memo and not isinstance(clauses_for_memo, list):
                clauses_for_memo = [clauses_for_memo]
            can_use_failed_memo = bool(clauses_for_memo) and self._clause_defs_are_pure(clauses_for_memo)
            if can_use_failed_memo:
                self._internal_log(f"[FAIL] clause={clause_name} reason=memoized_failed_state")
                return False

        if call_signature in self._successful_clause_results and can_use_success_cache:
            self._internal_log(f"[DEBUG] clause={clause_name} reason=memoized_success_state")
            return copy.deepcopy(self._successful_clause_results[call_signature])

        if current_depth > self.max_clause_call_depth:
            self._internal_log(
                f"[FAIL] clause={clause_name} reason=max_call_depth_exceeded depth={current_depth} limit={self.max_clause_call_depth}"
            )
            return False

        if same_call_count > self.max_same_clause_calls:
            self._internal_log(
                f"[FAIL] clause={clause_name} reason=repeated_call_limit_exceeded count={same_call_count} limit={self.max_same_clause_calls}"
            )
            return False

        self._clause_call_stack.append(call_signature)
        self._clause_call_counts[call_signature] = same_call_count

        try:
        # Check if clause exists
            if clause_name not in self.clauses:
                # Fallback: look for a Python function in solf_function.py
                py_result = self._try_python_function(clause_name, call_args)
                if py_result is not None:
                    return py_result
                fail_idx = caller_clause.execution_indices if caller_clause else []
                self._internal_log(f"[FAIL] clause={clause_name} reason=not_found execution_indices={fail_idx}")
                return None
        
            clauses = self.clauses[clause_name]
            if not isinstance(clauses, list):
                clauses = [clauses]

            can_use_success_cache = self._clause_defs_are_pure(clauses)
            if can_use_success_cache and call_signature in self._successful_clause_results:
                self._internal_log(f"[DEBUG] clause={clause_name} reason=memoized_success_state")
                return copy.deepcopy(self._successful_clause_results[call_signature])

            self._internal_log(f"[DEBUG] invoking clause={clause_name} with args={call_args}")

            any_success = False
            last_success_result = None
            last_fail_indices = []

            for idx, clause_def in enumerate(clauses):
                self._internal_log(f"[DEBUG] clause={clause_name} trying candidate #{idx}")
                self._internal_log(f"[DEBUG] clause={clause_name} parsed_body={clause_def.get('parsed_body')}")
                self._internal_log(f"[DEBUG] clause={clause_name} args={clause_def.get('args')}")

                if clause_def.get('parsed_body') is None:
                    self._internal_log(f"[FAIL] clause={clause_name} reason=parsed_body_is_None")
                    continue

                new_clause = Clause(
                    name=clause_name,
                    predicates=clause_def['parsed_body'],
                    arguments=clause_def['args']
                )
                new_clause.reset_execution()

                # Match and bind arguments — uses _match_arguments for proper atom/variable handling
                matched, bindings, overwrite_vars = self._match_arguments(
                    clause_def.get('args', []), call_args, caller_clause, call_arg_refs)
                if not matched:
                    self._internal_log(f"[DEBUG] clause={clause_name} candidate #{idx} args not matched, skipping")
                    continue
                self._internal_log(f"[DEBUG] clause={clause_name} bindings={bindings}")
                new_clause.variables.update(bindings)

                caller_state = (
                    caller_clause.save_state() if caller_clause
                    else copy.deepcopy(self.variables)
                )

                self._savepoint_seq += 1
                savepoint_name = f"{clause_name}_{current_depth}_{self._savepoint_seq}"
                solf_function_module = self._load_python_extension_module("solf_function")
                has_savepoint = bool(
                    solf_function_module is not None
                    and solf_function_module.create_savepoint(savepoint_name)
                )

                try:
                    clause_meta = clause_def.get('parsed_meta')
                    if self._active_debugger is not None and clause_meta is None:
                        clause_meta = self._ensure_clause_debug_metadata(clause_def)

                    pushed_frame = self._push_debug_frame(clause_name, clause_meta)
                    try:
                        result = self._execute_predicate_internal(clause_def['parsed_body'], new_clause)
                    finally:
                        if pushed_frame:
                            self._pop_debug_frame()

                    if isinstance(result, SOLFReturnValue):
                        result = result.value

                    self._internal_log(f"[DEBUG] clause={clause_name} result={result}")

                    if result is not False:
                        any_success = True
                        last_success_result = result

                        if has_savepoint:
                            solf_function_module.release_savepoint(savepoint_name)

                        # Propagate selected callee variable updates back into caller scope.
                        # This allows calls like save_crossing(_cbank, _crossing) to write
                        # the updated _cbank value back to the caller when names match.
                        if overwrite_vars:
                            target_context = caller_clause.variables if caller_clause else self.variables
                            for var_name in overwrite_vars:
                                if var_name in new_clause.variables:
                                    target_context[var_name] = new_clause.variables[var_name]

                        # If the ∃ operator requested stop-on-first-success, break immediately
                        stop_on_first = (
                            getattr(caller_clause, 'stop_on_first', False) if caller_clause
                            else self._stop_on_first
                        )
                        if stop_on_first:
                            self._internal_log(f"[DEBUG] clause={clause_name} stop_on_first: returning after first success")
                            break
                    else:
                        last_fail_indices = new_clause.last_failed_indices or list(new_clause.execution_indices)
                        self._internal_log(f"[FAIL] clause={clause_name} execution_indices={last_fail_indices}")
                        if has_savepoint:
                            solf_function_module.rollback_to_savepoint(savepoint_name)
                            solf_function_module.release_savepoint(savepoint_name)
                        if caller_clause:
                            caller_clause.restore_state(caller_state)
                        else:
                            self.variables = caller_state

                except Exception as e:
                    fail_idx = new_clause.last_failed_indices or list(new_clause.execution_indices)
                    self._internal_log(f"[FAIL] clause={clause_name} execution_indices={fail_idx} error={e}")
                    import traceback
                    if self.debug_enabled:
                        traceback.print_exc()
                    if has_savepoint:
                        solf_function_module.rollback_to_savepoint(savepoint_name)
                        solf_function_module.release_savepoint(savepoint_name)
                    if caller_clause:
                        caller_clause.restore_state(caller_state)
                    else:
                        self.variables = caller_state
                    continue

            if not any_success:
                if can_use_success_cache:
                    self._failed_clause_signatures.add(call_signature)
                self._internal_log(f"[FAIL] clause={clause_name} reason=all_candidates_failed execution_indices={last_fail_indices}")
                return False

            self._failed_clause_signatures.discard(call_signature)
            if can_use_success_cache:
                self._successful_clause_results[call_signature] = copy.deepcopy(last_success_result)
            return last_success_result
        finally:
            if self._clause_call_stack:
                self._clause_call_stack.pop()
            remaining = self._clause_call_counts.get(call_signature, 1) - 1
            if remaining > 0:
                self._clause_call_counts[call_signature] = remaining
            else:
                self._clause_call_counts.pop(call_signature, None)
    
    def _invoke_fact(self, fact_name, call_args=None):
        """
        Invoke/query a fact.
        
        Args:
            fact_name: Name of the fact
            call_args: Optional arguments for fact querying
            
        Returns:
            Fact data or query result
        """
        if fact_name not in self.facts:
            return None
        
        fact_data = self.facts[fact_name]
        return self._query_fact_data(fact_data, call_args)

    def _query_fact_data(self, fact_data, call_args=None):
        """Query fact-like data (loaded facts or runtime variable values)."""
        # No arguments - return entire fact payload
        if call_args is None or len(call_args) == 0:
            return fact_data

        # With arguments - perform lookup/matching
        if isinstance(fact_data, dict):
            # Dictionary-based fact - lookup by key
            if len(call_args) >= 1:
                key = call_args[0]
                # Resolve variable if needed
                if isinstance(key, str) and key.startswith('_'):
                    key = self.variables.get(key, key)
                
                if key in fact_data:
                    return fact_data[key]
                
                # Try 'get' key for query examples
                if 'get' in fact_data:
                    return fact_data['get']
            
            return None
        
        elif isinstance(fact_data, list):
            # List-based fact supports:
            # - scalar membership checks: man(Yeung) -> true/false
            # - wildcard retrieval: man(_) -> full list
            # - tuple checks: child_of(Yeung, Benjamin) -> true/false
            # - tuple wildcard pattern: child_of((Yeung, _)) -> matching tuples

            resolved_args = [self._resolve_fact_query_value(arg) for arg in call_args]

            if len(resolved_args) == 1:
                arg = resolved_args[0]

                # Wildcard argument requests all entries.
                if self._is_fact_wildcard(arg):
                    return fact_data

                # Pattern query with wildcards inside nested structures.
                if self._contains_fact_wildcard(arg):
                    return [item for item in fact_data if self._match_fact_pattern(arg, item)]

                # Direct membership check for scalar or structured values.
                return any(self._match_fact_pattern(arg, item) for item in fact_data)

            # Multiple arguments represent a tuple call pattern.
            tuple_pattern = tuple(resolved_args)
            if self._contains_fact_wildcard(tuple_pattern):
                return [item for item in fact_data if self._match_fact_pattern(tuple_pattern, item)]

            return any(self._match_fact_pattern(tuple_pattern, item) for item in fact_data)
        
        # Simple value fact
        return fact_data

    def _resolve_fact_query_value(self, value):
        """Resolve variable-based query operands while preserving wildcard placeholders."""
        if isinstance(value, str) and value.startswith('_') and value != '_':
            if value in self.variables:
                return self.variables.get(value)
        return value

    def _is_fact_wildcard(self, value):
        """Return True when value acts as a wildcard in fact queries."""
        if value == '_':
            return True
        if isinstance(value, str) and value.startswith('_') and value not in self.variables:
            return True
        return False

    def _contains_fact_wildcard(self, pattern):
        """Return True when a nested pattern contains any wildcard token."""
        if self._is_fact_wildcard(pattern):
            return True
        if isinstance(pattern, (list, tuple)):
            return any(self._contains_fact_wildcard(item) for item in pattern)
        if isinstance(pattern, dict):
            return any(self._contains_fact_wildcard(v) for v in pattern.values())
        return False

    def _match_fact_pattern(self, pattern, value):
        """Recursively match fact data against scalar/tuple/list/dict query patterns."""
        if self._is_fact_wildcard(pattern):
            return True

        if isinstance(pattern, tuple):
            if not isinstance(value, (tuple, list)) or len(value) != len(pattern):
                return False
            return all(self._match_fact_pattern(p, v) for p, v in zip(pattern, value))

        if isinstance(pattern, list):
            if not isinstance(value, list) or len(value) != len(pattern):
                return False
            return all(self._match_fact_pattern(p, v) for p, v in zip(pattern, value))

        if isinstance(pattern, dict):
            if not isinstance(value, dict):
                return False
            for key, pat_val in pattern.items():
                if key not in value:
                    return False
                if not self._match_fact_pattern(pat_val, value[key]):
                    return False
            return True

        return pattern == value

    def get_clause_definitions(self, clause_name):
        """Return loaded clause definitions for a clause name."""
        return self.clauses.get(clause_name, [])

    def invoke_clause(self, clause_name, call_args=None):
        """Public wrapper for invoking a loaded clause by name."""
        return self._invoke_clause(clause_name, call_args or [])
    
    def invoke(self, invocation_string):
        """
        Invoke a SOLF operation from a string.
        
        Args:
            invocation_string: A string representing the SOLF operation to perform.
            
        Returns:
            The result of the operation.
        """
        # This would typically parse the invocation string
        # For now, return a placeholder
        return f"Invoking: {invocation_string}"
