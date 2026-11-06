"""bb compile: a compiled program runs its entry function the way bb run does."""
import os
import subprocess
import sys
from pathlib import Path

BB = Path(__file__).parent.parent / 'bb.py'


def bb(runner, *args, stdin=None):
    return subprocess.run([sys.executable, str(BB), *args], capture_output=True, text=True,
                          input=stdin, env={**os.environ, **runner.env})


def test_a_compiled_program_gets_its_arguments_as_strings_and_prints_nothing_itself(cli_runner, tmp_path):
    source = tmp_path / 'show_args_eng.py'
    source.write_text(
        'def show_args(*argv):\n'
        '    """Print the arguments and their types."""\n'
        '    print(" ".join(f"{arg}:{type(arg).__name__}" for arg in argv))\n',
        encoding='utf-8')
    cli_runner.add(str(source), 'eng')
    assert bb(cli_runner, 'review', 'show_args@eng', stdin='y\n' * 5).returncode == 0
    program = tmp_path / 'show_args.py'
    assert bb(cli_runner, 'compile', '--output', str(program), 'show_args@eng').returncode == 0

    result = subprocess.run([sys.executable, str(program), '007', '1e3', 'nan', 'x'],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    # Not 7, 1000.0, nan: what was typed; and no "None" after the program's own output
    assert result.stdout == '007:str 1e3:str nan:str x:str\n'

    without_arguments = subprocess.run([sys.executable, str(program)], capture_output=True, text=True)
    assert without_arguments.stdout == '\n'  # called with no arguments, not a usage line


def test_compiled_functions_keep_their_names_unless_that_would_clash(cli_runner, tmp_path):
    for name, code in [
        ('double', 'def double(n):\n    """Twice n."""\n    return 2 * int(n)\n'),
        ('sum', 'def sum(values):\n    """Add up."""\n    total = 0\n    for v in values:\n        total += v\n    return total\n'),
    ]:
        source = tmp_path / f'{name}_eng.py'
        source.write_text(code, encoding='utf-8')
        cli_runner.add(str(source), 'eng')
    main = tmp_path / 'main_eng.py'
    main.write_text('import bb\n\ndouble = bb.latest("double")\nsum = bb.latest("sum")\n\n\n'
                    'def main(*argv):\n    """Print twice the total."""\n'
                    '    print(double(sum([int(a) for a in argv])))\n', encoding='utf-8')
    cli_runner.add(str(main), 'eng')
    assert bb(cli_runner, 'review', 'main@eng', stdin='y\n' * 10).returncode == 0
    program = tmp_path / 'main.py'
    assert bb(cli_runner, 'compile', '--output', str(program), 'main@eng').returncode == 0
    text = program.read_text()
    assert 'def double(' in text and 'def main(' in text  # plain names
    assert 'def sum(' not in text and 'def sum_' in text  # would shadow the builtin
    result = subprocess.run([sys.executable, str(program), '1', '2', '3'], capture_output=True, text=True)
    assert result.stdout == '12\n', result.stderr
