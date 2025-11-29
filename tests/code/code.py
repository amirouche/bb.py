"""
AST Field Type Mapping Generator

This module introspects Python's ast module to discover all AST node types
and their field types. This information is used to generate valid Python AST
trees that can be unparsed into parsable Python code.

The generator will be used in the Aston fuzzer for testing AST normalization.
"""

import ast
import random
import sys
from typing import Dict, List, Optional, Any


def introspect_ast_nodes() -> Dict[str, Dict[str, str]]:
    """
    Introspect all AST node types and their field types.

    Returns:
        Dictionary mapping node class names to their field types:
        {
            'ClassName': {
                'field_name': 'field_type',
                ...
            }
        }
    """
    node_field_types = {}

    # Get all AST node classes
    for name in dir(ast):
        obj = getattr(ast, name)

        # Skip non-classes and non-AST nodes
        if not isinstance(obj, type):
            continue
        if not issubclass(obj, ast.AST):
            continue
        if obj is ast.AST:
            continue

        # Get fields and their types
        if hasattr(obj, '_fields'):
            field_types = {}
            for field_name in obj._fields:
                field_type = infer_field_type(obj, field_name)
                field_types[field_name] = field_type

            if field_types:
                node_field_types[name] = field_types

    return node_field_types


def infer_field_type(node_class: type, field_name: str) -> str:
    """
    Infer the type of a field by examining the AST node class.

    Returns a string describing the field type:
    - 'identifier': str
    - 'int': int
    - 'string': str
    - 'constant': Any
    - 'expr': single expression node
    - 'stmt': single statement node
    - 'expr*': list of expression nodes
    - 'stmt*': list of statement nodes
    - 'expr?': optional expression node
    - etc.
    """
    # This is a manual mapping based on Python AST documentation
    # In practice, we need to inspect actual instances or refer to docs

    # Common patterns
    if field_name in ['lineno', 'col_offset', 'end_lineno', 'end_col_offset']:
        return 'int'
    if field_name in ['name', 'id', 'attr', 'arg', 'module']:
        return 'identifier'
    if field_name in ['asname']:
        return 'identifier?'
    if field_name == 'value':
        # Special handling for value field
        if node_class.__name__ in ['Constant', 'Num', 'Str', 'Bytes', 'NameConstant', 'Ellipsis']:
            return 'constant'
        elif node_class.__name__ in ['Assign', 'AugAssign', 'AnnAssign', 'Return']:
            return 'expr'
        elif node_class.__name__ in ['Attribute', 'Subscript']:
            return 'expr'
        else:
            return 'unknown'

    # Lists
    if field_name in ['body', 'orelse', 'finalbody']:
        # Lambda.body is expr, not stmt*
        if node_class.__name__ == 'Lambda' and field_name == 'body':
            return 'expr'
        # IfExp.body and IfExp.orelse are expr, not stmt*
        if node_class.__name__ == 'IfExp' and field_name in ['body', 'orelse']:
            return 'expr'
        return 'stmt*'
    if field_name in ['elts', 'keys', 'values', 'comparators']:
        return 'expr*'
    if field_name in ['args', 'posonlyargs', 'kwonlyargs']:
        # FunctionDef/AsyncFunctionDef/Lambda.args is 'arguments', not 'arg*'
        if node_class.__name__ in ['FunctionDef', 'AsyncFunctionDef', 'Lambda'] and field_name == 'args':
            return 'arguments'
        return 'arg*'
    if field_name in ['names']:
        # Import/ImportFrom use alias*, Global/Nonlocal use identifier*
        if node_class.__name__ in ['Import', 'ImportFrom']:
            return 'alias*'
        elif node_class.__name__ in ['Global', 'Nonlocal']:
            return 'identifier*'
        else:
            return 'alias*'  # default
    if field_name in ['bases', 'keywords', 'decorator_list']:
        return 'expr*'
    if field_name in ['targets']:
        return 'expr*'
    if field_name in ['handlers']:
        return 'excepthandler*'
    if field_name in ['items']:
        return 'withitem*'
    if field_name in ['ifs']:
        return 'expr*'
    if field_name in ['generators']:
        return 'comprehension*'

    # Optional fields
    if field_name == 'returns':
        return 'expr?'
    if field_name == 'type_comment':
        return 'string?'
    if field_name in ['defaults', 'kw_defaults']:
        return 'expr*'

    # Single nodes
    if field_name in ['test', 'iter', 'target', 'left', 'right', 'func', 'lower', 'upper', 'step']:
        return 'expr'
    if field_name in ['cause']:
        return 'expr?'
    if field_name in ['exc']:
        return 'expr?'
    if field_name in ['type']:
        return 'expr?'
    if field_name in ['slice']:
        return 'expr'
    if field_name == 'annotation':
        # AnnAssign.annotation is required, others might be optional
        if node_class.__name__ == 'AnnAssign':
            return 'expr'
        else:
            return 'expr?'
    if field_name in ['simple']:
        return 'int'

    # Operators and context
    if field_name == 'op':
        # Check node class to determine operator type
        if node_class.__name__ == 'BoolOp':
            return 'boolop'
        elif node_class.__name__ == 'UnaryOp':
            return 'unaryop'
        else:
            return 'operator'
    if field_name == 'ops':
        # Compare uses cmpop*, others use operator*
        if node_class.__name__ == 'Compare':
            return 'cmpop*'
        else:
            return 'operator*'
    if field_name in ['ctx']:
        return 'expr_context'
    if field_name in ['boolop']:
        return 'boolop'
    if field_name in ['unaryop']:
        return 'unaryop'
    if field_name in ['cmpop']:
        return 'cmpop*'

    # Special cases
    if field_name == 'kind':
        return 'string?'
    if field_name == 'n':
        return 'constant'
    if field_name == 's':
        return 'constant'
    if field_name in ['vararg', 'kwarg']:
        return 'arg?'

    # Default: try to infer from context
    return 'unknown'


def get_usable_nodes() -> Dict[str, Dict[str, str]]:
    """
    Get all AST nodes that don't have unknown field types.

    Returns:
        Dictionary of usable nodes with their field types.
    """
    all_nodes = introspect_ast_nodes()
    usable_nodes = {}

    # Nodes with complex inter-field dependencies or that cause frequent syntax errors
    excluded_nodes = {
        'Raise',  # cause requires exc to be set
        'Delete',  # requires valid delete targets (often generates invalid literals)
        'Global',  # requires identifiers, not expressions
        'Nonlocal',  # requires identifiers, not expressions
    }

    for node_name, fields in all_nodes.items():
        # Skip excluded nodes
        if node_name in excluded_nodes:
            continue
        # Skip nodes with unknown field types
        if any(field_type == 'unknown' for field_type in fields.values()):
            continue
        usable_nodes[node_name] = fields

    return usable_nodes


class ASTGenerator:
    """Random AST generator with deterministic seed and energy budget."""

    def __init__(self, seed: int, max_depth: int = 3, energy: Optional[int] = None):
        self.rng = random.Random(seed)
        self.max_depth = max_depth
        self.energy = energy if energy is not None else 1000
        self.usable_nodes = get_usable_nodes()

        # Categorize nodes by base type
        self.expr_nodes = []
        self.stmt_nodes = []
        self.operator_nodes = []
        self.boolop_nodes = []
        self.unaryop_nodes = []
        self.cmpop_nodes = []
        self.expr_context_nodes = []

        for name in dir(ast):
            obj = getattr(ast, name)
            if not isinstance(obj, type) or not issubclass(obj, ast.AST):
                continue

            # Only use nodes that are in usable_nodes
            if name not in self.usable_nodes:
                continue

            # Categorize by base class
            if hasattr(obj, '__bases__'):
                bases = [b.__name__ for b in obj.__bases__]
                if 'expr' in bases:
                    self.expr_nodes.append(name)
                elif 'stmt' in bases:
                    self.stmt_nodes.append(name)
                elif 'operator' in bases:
                    self.operator_nodes.append(name)
                elif 'boolop' in bases:
                    self.boolop_nodes.append(name)
                elif 'unaryop' in bases:
                    self.unaryop_nodes.append(name)
                elif 'cmpop' in bases:
                    self.cmpop_nodes.append(name)
                elif 'expr_context' in bases:
                    self.expr_context_nodes.append(name)

    def consume_energy(self, amount: int = 1) -> bool:
        """
        Consume energy and return True if successful, False if exhausted.
        """
        if self.energy <= 0:
            return False
        self.energy -= amount
        return True

    def generate_identifier(self) -> Optional[str]:
        """Generate a random valid Python identifier."""
        if not self.consume_energy():
            return None
        letters = 'abcdefghijklmnopqrstuvwxyz'
        length = self.rng.randint(1, 8)
        return ''.join(self.rng.choice(letters) for _ in range(length))

    def generate_constant(self) -> Any:
        """Generate a random constant value (for use in non-assignment contexts)."""
        if not self.consume_energy():
            return None
        choices = [
            lambda: self.rng.randint(-100, 100),
            lambda: self.rng.random() * 100,
            lambda: self.generate_identifier(),
            lambda: True,
            lambda: False,
            lambda: None,
        ]
        return self.rng.choice(choices)()

    def generate_valid_target(self, depth: int) -> Optional[ast.expr]:
        """
        Generate a valid assignment/loop target (only Name nodes with Store context).
        Avoids constants which cause syntax errors.
        """
        if not self.consume_energy():
            return None
        identifier = self.generate_identifier()
        if identifier is None:
            return None
        return ast.Name(id=identifier, ctx=ast.Store())

    def generate_field_value(self, field_type: str, depth: int, field_name: str = '') -> Any:
        """Generate a value for a specific field type."""
        if not self.consume_energy():
            return None

        if depth >= self.max_depth:
            # At max depth, generate simple values only
            if field_type == 'identifier':
                return self.generate_identifier()
            elif field_type == 'identifier?':
                return None if self.rng.random() < 0.5 else self.generate_identifier()
            elif field_type == 'int':
                return self.rng.randint(0, 10)
            elif field_type == 'constant':
                return self.generate_constant()
            elif field_type in ['string', 'string?']:
                return None if field_type == 'string?' and self.rng.random() < 0.5 else self.generate_identifier()
            elif field_type in ['expr', 'stmt']:
                # Return simplest possible node
                return self.generate_simple_node(field_type)
            elif field_type.endswith('*'):
                return []
            elif field_type.endswith('?'):
                return None
            else:
                return None

        # Regular generation
        if field_type == 'identifier':
            return self.generate_identifier()
        elif field_type == 'identifier?':
            return None if self.rng.random() < 0.5 else self.generate_identifier()
        elif field_type == 'identifier*':
            count = self.rng.randint(1, 2)
            ids = []
            for _ in range(count):
                id_val = self.generate_identifier()
                if id_val:
                    ids.append(id_val)
            return ids
        elif field_type == 'int':
            return self.rng.randint(0, 10)
        elif field_type == 'constant':
            return self.generate_constant()
        elif field_type == 'string':
            return self.generate_identifier()
        elif field_type == 'string?':
            return None if self.rng.random() < 0.5 else self.generate_identifier()
        elif field_type == 'expr':
            # Special case for assignment/loop targets
            if field_name in ['target', 'targets']:
                return self.generate_valid_target(depth)
            return self.generate_expr(depth + 1)
        elif field_type == 'expr?':
            return None if self.rng.random() < 0.5 else self.generate_expr(depth + 1)
        elif field_type == 'expr*':
            # Special case for assignment/delete targets
            if field_name in ['targets']:
                count = self.rng.randint(1, 2)
                return [self.generate_valid_target(depth + 1) for _ in range(count) if self.consume_energy()]
            count = self.rng.randint(0, 2)
            exprs = []
            for _ in range(count):
                expr = self.generate_expr(depth + 1)
                if expr is not None:
                    exprs.append(expr)
            return exprs
        elif field_type == 'stmt':
            return self.generate_stmt(depth + 1)
        elif field_type == 'stmt*':
            count = self.rng.randint(1, 3)
            stmts = []
            for _ in range(count):
                stmt = self.generate_stmt(depth + 1)
                if stmt is not None:
                    stmts.append(stmt)
            return stmts if stmts else [ast.Pass()]
        elif field_type == 'operator':
            return self.generate_operator()
        elif field_type == 'operator*':
            count = self.rng.randint(1, 2)
            return [self.generate_operator() for _ in range(count) if self.consume_energy()]
        elif field_type == 'boolop':
            return self.generate_boolop()
        elif field_type == 'unaryop':
            return self.generate_unaryop()
        elif field_type == 'cmpop*':
            count = self.rng.randint(1, 2)
            return [self.generate_cmpop() for _ in range(count) if self.consume_energy()]
        elif field_type == 'expr_context':
            return self.generate_expr_context()
        elif field_type == 'arg*':
            count = self.rng.randint(0, 2)
            args = []
            for _ in range(count):
                arg = self.generate_arg(depth + 1)
                if arg:
                    args.append(arg)
            return args
        elif field_type == 'arg?':
            return None if self.rng.random() < 0.7 else self.generate_arg(depth + 1)
        elif field_type == 'arguments':
            return self.generate_arguments(depth + 1)
        elif field_type == 'alias*':
            count = self.rng.randint(1, 2)
            aliases = []
            for _ in range(count):
                alias = self.generate_alias(depth + 1)
                if alias:
                    aliases.append(alias)
            return aliases
        elif field_type == 'excepthandler*':
            count = self.rng.randint(1, 2)
            return [self.generate_excepthandler(depth + 1) for _ in range(count) if self.consume_energy()]
        elif field_type == 'withitem*':
            count = self.rng.randint(1, 2)
            return [self.generate_withitem(depth + 1) for _ in range(count) if self.consume_energy()]
        elif field_type == 'comprehension*':
            comp = self.generate_comprehension(depth + 1)
            return [comp] if comp else []
        else:
            return None

    def generate_simple_node(self, node_type: str) -> ast.AST:
        """Generate simplest possible node of given type."""
        if node_type == 'expr':
            return ast.Constant(value=42)
        elif node_type == 'stmt':
            return ast.Pass()
        return ast.Constant(value=None)

    def generate_expr(self, depth: int = 0) -> Optional[ast.expr]:
        """Generate a random expression node with support for composition and chaining."""
        if not self.consume_energy():
            return None
        if depth >= self.max_depth or not self.expr_nodes:
            const_val = self.generate_constant()
            return ast.Constant(value=const_val) if const_val is not None else ast.Constant(value=42)

        # Small chance to generate function composition or method chaining (only at low depth)
        if depth < 2 and self.rng.random() < 0.15:  # 15% chance
            if self.rng.random() < 0.5:
                # Try to generate function composition
                composed = self.generate_function_composition(depth)
                if composed:
                    return composed
            else:
                # Try to generate method chaining
                chained = self.generate_method_chain(depth)
                if chained:
                    return chained

        # Prefer simpler expressions at higher depths
        if depth > 1:
            simple_exprs = ['Constant', 'Name']
            available = [n for n in simple_exprs if n in self.expr_nodes]
            if available:
                node_name = self.rng.choice(available)
            else:
                node_name = self.rng.choice(self.expr_nodes)
        else:
            node_name = self.rng.choice(self.expr_nodes)

        return self.generate_node(node_name, depth)

    def generate_function_composition(self, depth: int) -> Optional[ast.Call]:
        """
        Generate function composition: outer_func(inner_func(arg))
        """
        if not self.consume_energy(3):  # Costs more energy
            return None

        # Generate outer function (Name or Attribute)
        if self.rng.random() < 0.7:
            # Simple name
            func_name = self.generate_identifier()
            if not func_name:
                return None
            outer_func = ast.Name(id=func_name, ctx=ast.Load())
        else:
            # Attribute (e.g., obj.method)
            obj_name = self.generate_identifier()
            attr_name = self.generate_identifier()
            if not obj_name or not attr_name:
                return None
            outer_func = ast.Attribute(
                value=ast.Name(id=obj_name, ctx=ast.Load()),
                attr=attr_name,
                ctx=ast.Load()
            )

        # Generate inner call
        inner_func_name = self.generate_identifier()
        if not inner_func_name:
            return None
        inner_arg = self.generate_expr(depth + 2)
        if not inner_arg:
            return None

        inner_call = ast.Call(
            func=ast.Name(id=inner_func_name, ctx=ast.Load()),
            args=[inner_arg],
            keywords=[]
        )

        # Generate outer call with inner call as argument
        return ast.Call(
            func=outer_func,
            args=[inner_call],
            keywords=[]
        )

    def generate_method_chain(self, depth: int) -> Optional[ast.Call]:
        """
        Generate method chaining: obj.method1().method2()
        """
        if not self.consume_energy(3):  # Costs more energy
            return None

        # Generate base object
        obj_name = self.generate_identifier()
        if not obj_name:
            return None
        base = ast.Name(id=obj_name, ctx=ast.Load())

        # First method call
        method1_name = self.generate_identifier()
        if not method1_name:
            return None

        first_call = ast.Call(
            func=ast.Attribute(
                value=base,
                attr=method1_name,
                ctx=ast.Load()
            ),
            args=[],
            keywords=[]
        )

        # Possibly add a second method in the chain
        if self.rng.random() < 0.6:  # 60% chance for second method
            method2_name = self.generate_identifier()
            if not method2_name:
                return first_call

            second_call = ast.Call(
                func=ast.Attribute(
                    value=first_call,
                    attr=method2_name,
                    ctx=ast.Load()
                ),
                args=[],
                keywords=[]
            )
            return second_call

        return first_call

    def generate_stmt(self, depth: int = 0) -> Optional[ast.stmt]:
        """Generate a random statement node."""
        if not self.consume_energy():
            return None
        if depth >= self.max_depth or not self.stmt_nodes:
            return ast.Pass()

        # Prefer simpler statements at higher depths
        if depth > 1:
            # At higher depths, strongly prefer simple statements
            simple_stmts = ['Pass', 'Expr']
            available = [n for n in simple_stmts if n in self.stmt_nodes]
            if available and self.rng.random() < 0.7:  # 70% chance to use simple stmt
                node_name = self.rng.choice(available)
            else:
                node_name = self.rng.choice(self.stmt_nodes)
        else:
            node_name = self.rng.choice(self.stmt_nodes)

        return self.generate_node(node_name, depth)

    def generate_operator(self) -> ast.operator:
        """Generate a random operator."""
        if not self.operator_nodes:
            return ast.Add()
        node_name = self.rng.choice(self.operator_nodes)
        return getattr(ast, node_name)()

    def generate_boolop(self) -> ast.boolop:
        """Generate a random boolean operator."""
        if not self.boolop_nodes:
            return ast.And()
        node_name = self.rng.choice(self.boolop_nodes)
        return getattr(ast, node_name)()

    def generate_unaryop(self) -> ast.unaryop:
        """Generate a random unary operator."""
        if not self.unaryop_nodes:
            return ast.Not()
        node_name = self.rng.choice(self.unaryop_nodes)
        return getattr(ast, node_name)()

    def generate_cmpop(self) -> ast.cmpop:
        """Generate a random comparison operator."""
        if not self.cmpop_nodes:
            return ast.Eq()
        node_name = self.rng.choice(self.cmpop_nodes)
        return getattr(ast, node_name)()

    def generate_expr_context(self) -> ast.expr_context:
        """Generate a random expression context."""
        if not self.expr_context_nodes:
            return ast.Load()
        node_name = self.rng.choice(self.expr_context_nodes)
        return getattr(ast, node_name)()

    def generate_arg(self, depth: int) -> ast.arg:
        """Generate a function argument."""
        return ast.arg(arg=self.generate_identifier(), annotation=None)

    def generate_arguments(self, depth: int) -> ast.arguments:
        """Generate a function arguments object."""
        # Generate simple arguments with just args, keep others empty/None
        num_args = self.rng.randint(0, 2)
        args = [self.generate_arg(depth) for _ in range(num_args)]
        return ast.arguments(
            posonlyargs=[],
            args=args,
            vararg=None,
            kwonlyargs=[],
            kw_defaults=[],
            kwarg=None,
            defaults=[]
        )

    def generate_alias(self, depth: int) -> ast.alias:
        """Generate an import alias."""
        return ast.alias(name=self.generate_identifier(), asname=None)

    def generate_excepthandler(self, depth: int) -> ast.ExceptHandler:
        """Generate an exception handler."""
        return ast.ExceptHandler(
            type=None,
            name=None,
            body=[ast.Pass()]
        )

    def generate_withitem(self, depth: int) -> ast.withitem:
        """Generate a with item."""
        return ast.withitem(
            context_expr=self.generate_expr(depth + 1),
            optional_vars=None
        )

    def generate_comprehension(self, depth: int) -> Optional[ast.comprehension]:
        """Generate a comprehension."""
        if not self.consume_energy():
            return None
        target = self.generate_valid_target(depth)
        iter_expr = self.generate_expr(depth + 1)
        if target is None or iter_expr is None:
            return None
        return ast.comprehension(
            target=target,
            iter=iter_expr,
            ifs=[],
            is_async=0
        )

    def generate_node(self, node_name: str, depth: int) -> Optional[ast.AST]:
        """Generate a specific AST node by name."""
        if not self.consume_energy():
            return None

        # Special handling for nodes with ordering constraints
        if node_name == 'Try':
            return self.generate_try(depth)
        elif node_name == 'TryStar':
            return self.generate_try_star(depth)

        if node_name not in self.usable_nodes:
            # Fallback to simple node
            if node_name in self.expr_nodes or depth >= self.max_depth:
                return ast.Constant(value=42)
            else:
                return ast.Pass()

        node_class = getattr(ast, node_name)
        field_types = self.usable_nodes[node_name]

        # Generate values for all fields
        kwargs = {}
        for field_name, field_type in field_types.items():
            kwargs[field_name] = self.generate_field_value(field_type, depth, field_name)

        try:
            return node_class(**kwargs)
        except Exception:
            # On failure, return simple fallback
            if node_name in self.expr_nodes:
                return ast.Constant(value=42)
            else:
                return ast.Pass()

    def generate_try(self, depth: int) -> Optional[ast.Try]:
        """Generate a Try node with properly ordered except handlers."""
        if not self.consume_energy():
            return None

        # Generate body
        body = []
        num_stmts = self.rng.randint(1, 2)
        for _ in range(num_stmts):
            stmt = self.generate_stmt(depth + 1)
            if stmt:
                body.append(stmt)
        if not body:
            body = [ast.Pass()]

        # Generate exception handlers (1-3)
        # Ensure bare except (type=None) comes last
        num_handlers = self.rng.randint(1, 3)
        handlers = []

        # First generate handlers with types
        for i in range(num_handlers - 1):
            if self.consume_energy():
                exc_type = self.generate_expr(depth + 1) if self.rng.random() < 0.7 else None
                handler = ast.ExceptHandler(
                    type=exc_type,
                    name=None,
                    body=[ast.Pass()]
                )
                handlers.append(handler)

        # Last handler - might be bare
        if self.consume_energy():
            # 30% chance for bare except as last handler
            exc_type = None if self.rng.random() < 0.3 else self.generate_expr(depth + 1)
            handler = ast.ExceptHandler(
                type=exc_type,
                name=None,
                body=[ast.Pass()]
            )
            handlers.append(handler)

        if not handlers:
            handlers = [ast.ExceptHandler(type=None, name=None, body=[ast.Pass()])]

        # Generate orelse and finalbody
        orelse = []
        if self.rng.random() < 0.3:
            orelse_stmt = self.generate_stmt(depth + 1)
            if orelse_stmt:
                orelse.append(orelse_stmt)

        finalbody = []
        if self.rng.random() < 0.3:
            final_stmt = self.generate_stmt(depth + 1)
            if final_stmt:
                finalbody.append(final_stmt)

        return ast.Try(
            body=body,
            handlers=handlers,
            orelse=orelse,
            finalbody=finalbody
        )

    def generate_try_star(self, depth: int) -> Optional[ast.TryStar]:
        """Generate a TryStar node with properly ordered except handlers."""
        if not self.consume_energy():
            return None

        # Generate body
        body = []
        num_stmts = self.rng.randint(1, 2)
        for _ in range(num_stmts):
            stmt = self.generate_stmt(depth + 1)
            if stmt:
                body.append(stmt)
        if not body:
            body = [ast.Pass()]

        # Generate exception handlers (1-2)
        # Ensure bare except (type=None) comes last
        num_handlers = self.rng.randint(1, 2)
        handlers = []

        # First generate handlers with types
        for i in range(num_handlers - 1):
            if self.consume_energy():
                exc_type = self.generate_expr(depth + 1) if self.rng.random() < 0.7 else None
                handler = ast.ExceptHandler(
                    type=exc_type,
                    name=None,
                    body=[ast.Pass()]
                )
                handlers.append(handler)

        # Last handler - might be bare
        if self.consume_energy():
            # 30% chance for bare except as last handler
            exc_type = None if self.rng.random() < 0.3 else self.generate_expr(depth + 1)
            handler = ast.ExceptHandler(
                type=exc_type,
                name=None,
                body=[ast.Pass()]
            )
            handlers.append(handler)

        if not handlers:
            handlers = [ast.ExceptHandler(type=None, name=None, body=[ast.Pass()])]

        # Generate orelse and finalbody
        orelse = []
        if self.rng.random() < 0.3:
            orelse_stmt = self.generate_stmt(depth + 1)
            if orelse_stmt:
                orelse.append(orelse_stmt)

        finalbody = []
        if self.rng.random() < 0.3:
            final_stmt = self.generate_stmt(depth + 1)
            if final_stmt:
                finalbody.append(final_stmt)

        return ast.TryStar(
            body=body,
            handlers=handlers,
            orelse=orelse,
            finalbody=finalbody
        )

    def generate_module(self) -> Optional[ast.Module]:
        """
        Generate a random Module node.

        Module contains:
        - One or more import statements (Import or ImportFrom)
        - At most one FunctionDef or AsyncFunctionDef (possibly with decorators)
        """
        if not self.consume_energy():
            return None

        body = []

        # Generate 1-3 import statements
        num_imports = self.rng.randint(1, 3)
        for _ in range(num_imports):
            if not self.consume_energy():
                break
            # Choose between Import and ImportFrom
            if self.rng.random() < 0.5 and 'Import' in self.usable_nodes:
                import_stmt = self.generate_node('Import', 0)
                if import_stmt:
                    body.append(import_stmt)
            elif 'ImportFrom' in self.usable_nodes:
                import_stmt = self.generate_node('ImportFrom', 0)
                if import_stmt:
                    body.append(import_stmt)

        # Optionally generate one FunctionDef or AsyncFunctionDef
        if self.rng.random() < 0.8:  # 80% chance to include a function
            if self.rng.random() < 0.5 and 'FunctionDef' in self.usable_nodes:
                func = self.generate_function_def(0)
                if func:
                    body.append(func)
            elif 'AsyncFunctionDef' in self.usable_nodes:
                func = self.generate_async_function_def(0)
                if func:
                    body.append(func)

        # Ensure we have at least one import
        if not body:
            if 'Import' in self.usable_nodes:
                import_stmt = self.generate_node('Import', 0)
                if import_stmt:
                    body.append(import_stmt)
            else:
                return None

        return ast.Module(body=body, type_ignores=[])

    def generate_function_def(self, depth: int) -> Optional[ast.FunctionDef]:
        """Generate a FunctionDef node, possibly with decorators."""
        if not self.consume_energy():
            return None

        name = self.generate_identifier()
        if not name:
            return None

        args = self.generate_arguments(depth + 1)
        if not args:
            return None

        # Generate function body (1-3 statements)
        num_stmts = self.rng.randint(1, 3)
        body_stmts = []
        for _ in range(num_stmts):
            stmt = self.generate_stmt(depth + 1)
            if stmt:
                body_stmts.append(stmt)
        if not body_stmts:
            body_stmts = [ast.Pass()]

        # Optionally add decorators (0-2)
        decorator_list = []
        if self.rng.random() < 0.3:  # 30% chance of decorators
            num_decorators = self.rng.randint(1, 2)
            for _ in range(num_decorators):
                decorator = self.generate_expr(depth + 1)
                if decorator:
                    decorator_list.append(decorator)

        return ast.FunctionDef(
            name=name,
            args=args,
            body=body_stmts,
            decorator_list=decorator_list,
            returns=None,
            type_comment=None
        )

    def generate_async_function_def(self, depth: int) -> Optional[ast.AsyncFunctionDef]:
        """Generate an AsyncFunctionDef node, possibly with decorators."""
        if not self.consume_energy():
            return None

        name = self.generate_identifier()
        if not name:
            return None

        args = self.generate_arguments(depth + 1)
        if not args:
            return None

        # Generate function body (1-3 statements)
        num_stmts = self.rng.randint(1, 3)
        body_stmts = []
        for _ in range(num_stmts):
            stmt = self.generate_stmt(depth + 1)
            if stmt:
                body_stmts.append(stmt)
        if not body_stmts:
            body_stmts = [ast.Pass()]

        # Optionally add decorators (0-2)
        decorator_list = []
        if self.rng.random() < 0.3:  # 30% chance of decorators
            num_decorators = self.rng.randint(1, 2)
            for _ in range(num_decorators):
                decorator = self.generate_expr(depth + 1)
                if decorator:
                    decorator_list.append(decorator)

        return ast.AsyncFunctionDef(
            name=name,
            args=args,
            body=body_stmts,
            decorator_list=decorator_list,
            returns=None,
            type_comment=None
        )


def generate(seed: int, energy: Optional[int] = None) -> Optional[str]:
    """
    Generate a random AST module and return its unparsed code.

    Args:
        seed: Random seed for deterministic generation
        energy: Energy budget for generation (decrements for each AST node/scalar).
                Returns None if energy reaches 0 during generation.
                Default: 1000

    Returns:
        Unparsed Python code string, or None if generation fails or energy exhausted
    """
    try:
        generator = ASTGenerator(seed, energy=energy)
        module = generator.generate_module()
        if module is None:
            return None
        # Fix missing location information
        ast.fix_missing_locations(module)
        code = ast.unparse(module)
        # Validate the generated code by trying to parse it
        try:
            compile(code, '<generated>', 'exec')
            return code
        except SyntaxError:
            # Generated code has syntax errors, return None
            return None
    except Exception:
        return None


def generate_field_type_mapping() -> str:
    """
    Generate the FIELD TYPE MAPPING section.

    Returns:
        Formatted string documenting all node field types.
    """
    field_types = introspect_ast_nodes()

    output = []
    output.append("FIELD TYPE MAPPING")
    output.append("=" * 80)

    for node_name in sorted(field_types.keys()):
        fields = field_types[node_name]
        output.append(f"\n{node_name}:")
        for field_name, field_type in sorted(fields.items()):
            output.append(f"  {field_name}: {field_type}")

    return '\n'.join(output)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Generate random Python AST code')
    parser.add_argument('--seed', type=int, help='Random seed for deterministic generation')
    parser.add_argument('--energy', type=int, help='Energy budget for generation (default: 1000)')
    parser.add_argument('--mapping', action='store_true', help='Print field type mapping instead of generating code')
    args = parser.parse_args()

    if args.mapping:
        print(generate_field_type_mapping())
    elif args.seed is not None:
        code = generate(args.seed, energy=args.energy)
        if code is None:
            print("Error: Failed to generate valid AST code", file=sys.stderr)
            sys.exit(1)
        else:
            print(code)
    else:
        # Default: print field type mapping
        print(generate_field_type_mapping())
