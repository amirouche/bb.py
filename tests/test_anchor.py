"""
Tests for bb anchor: OpenTimestamps proofs next to the records.

The proofs are read by bb itself (anchor_parse, anchor_state); the client
that makes them is replaced here by a stand-in writing the same format.
"""
import hashlib
import sys

import pytest

import bb

# A proof made by `ots stamp` (opentimestamps-client 0.8.0) for a file holding "a\n":
# four calendars have it, no Bitcoin block yet
REAL_PENDING_PROOF = bytes.fromhex(
    '004f70656e54696d657374616d7073000050726f6f6600bf89e2e884e89294010887428fc522803d31065e7bce3cf03fe475'
    '096631e5e07bbd7a0fde60c4cf25c7f0109436ab5fb1d518daf6cb317db280c62308f02068655f5ca5d78402b0b369ebec79'
    '12e2c3952cc8ff2da15e63aee1a7beda29f808fff0108664e3272b9b0cf7796633cadc5f3df308f1202325421fd0a03b662b'
    'c46739256e69bb73ba2668cd0c6bb19c327318a1264c9a08f1205cfe6bac62fee1498e7fda403b2ab8de06569fa3a1035c37'
    '5ff0378cb58e20e808f1204f3407ca5231c5dc8ba5a18a4fecb562378657b6ea6df03043775de8602c6ffa08f0208df6f2cf'
    'f50048fb5133bf35ef0a11846100b6bae1aba08090c2e40483b2628308f1046b04b5daf008b96a234d6f4f4ba50083dfe30d'
    '2ef90c8e292868747470733a2f2f66696e6e65792e63616c656e6461722e657465726e69747977616c6c2e636f6dfff0108a'
    'dfc00e62629207eca26c57279ac7e108f12046a2a8bfc40ed894743b9e4986a4a48bbc48fb5776205ac82fc2db33ba064904'
    '08f120297a4a62aca4520fd45d64773f617db05b1372dc2a50d6081c831427bed4e03108f020e856a281272f213b986a5bbb'
    '9ce08f93d8e1dd8d2a3c4de69db00818d3031efe08f1046b04b5dbf0084ffec1d34cb1205b0083dfe30d2ef90c8e23226874'
    '7470733a2f2f6274632e63616c656e6461722e636174616c6c6178792e636f6dfff008c119c5ae72b7403e08f020c4495a93'
    'f2dbbeae0d1c326939344a8f09c1b180f6468f66dc01a8abaa19448108f010385321a99389b0ccd3707007d50900d008f120'
    '6b8974a9094cdcb83cd2970c9b1ed227485c00a4e5fcc3b3f1164083f2015ff708f1202ad54ad3bdce3629d511d73b96ac31'
    '9383be0ad11b5f36bd1a55dff34d21012d08f020887b6b830dc41d9d824c47423003d4c272d5987fc7bc810b6e0c835528d3'
    '119608f1046b04b5dbf0085171118dba20c2d00083dfe30d2ef90c8e2c2b68747470733a2f2f626f622e6274632e63616c65'
    '6e6461722e6f70656e74696d657374616d70732e6f7267f008ec6f1f6c88f721ad08f010b5eb2062263cf34b446bcb02952c'
    '21f408f1205b8943d30a3f43c33132cc716a709c48cec8e924ddcf9f9492a037ef0a9b3daa08f120ad5af8348cbc4eae2246'
    '28d8174ba4a01c741e5d063100df0377583350d1f8e508f020de69f075c0c45f9ef2ae54819358c828e45471ef6158233ca2'
    '95b6fabdbc870e08f1046b04b5dbf0083a9e15cc6b5485a80083dfe30d2ef90c8e2e2d68747470733a2f2f616c6963652e62'
    '74632e63616c656e6461722e6f70656e74696d657374616d70732e6f7267'
)

HEADER = bb.ANCHOR_MAGIC + b'\x01\x08'
PENDING = b'\x00' + bb.ANCHOR_ATTESTATION_PENDING + b'\x19\x18https://calendar.example'

STAND_IN = '''
import hashlib, os, sys
HEADER = bytes.fromhex(%r)
PENDING = bytes.fromhex(%r)
def proof(path, attestation):
    with open(path, 'rb') as f:
        return HEADER + hashlib.sha256(f.read()).digest() + attestation
command, paths = sys.argv[1], [a for a in sys.argv[2:] if not a.startswith('-')]
if command == 'stamp':
    for path in paths:
        with open(path + '.ots', 'xb') as f:
            f.write(proof(path, PENDING))
elif command == 'upgrade':
    for path in paths:
        os.replace(path, path + '.bak')
        with open(path, 'wb') as f:
            f.write(proof(path[:-4], bytes.fromhex('000588960d73d7190103f08c35')))
    print("Success! Timestamp complete", file=sys.stderr)
elif command == 'verify':
    if os.environ.get('STAND_IN_REFUSES'):
        sys.exit("Could not connect to local Bitcoin node")
    print("Success! Bitcoin block 870000 attests existence as of 2026-11-23 UTC", file=sys.stderr)
''' % (HEADER.hex(), PENDING.hex())


def bitcoin(height_varuint: bytes) -> bytes:
    """A Bitcoin attestation of the block whose height is given as a varuint."""
    return b'\x00' + bb.ANCHOR_ATTESTATION_BITCOIN + bytes([len(height_varuint)]) + height_varuint


@pytest.fixture
def stand_in(tmp_path, monkeypatch):
    """Have bb run a stand-in for the OpenTimestamps client."""
    script = tmp_path / 'ots_stand_in.py'
    script.write_text(STAND_IN, encoding='utf-8')
    monkeypatch.setenv('BB_OTS', f"{sys.executable} {script}")


def _add(tmp_path, source="def one():\n    return 1\n", name='one.py'):
    path = tmp_path / name
    path.write_text(source, encoding='utf-8')
    bb.code_add(f"{path}@eng")


def _states():
    return sorted(bb.anchor_state(path)[0] for path in bb.anchor_list_files())


def test_anchor_parse_reads_a_real_proof():
    digest, heights, pending = bb.anchor_parse(REAL_PENDING_PROOF)
    assert digest == hashlib.sha256(b"a\n").digest()
    assert (heights, pending) == ([], 4)


def test_anchor_parse_reads_block_heights_through_operations_and_forks():
    digest = bytes(32)
    # sha256, then a fork: append 2 bytes then block 870000; prepend 1 byte, keccak, then block 5, and a pending one
    proof = (HEADER + digest + b'\x08' + b'\xff' + b'\xf0\x02ab' + bitcoin(b'\xf0\x8c\x35')
             + b'\xff' + b'\xf1\x01c' + b'\x67' + bitcoin(b'\x05') + PENDING[:])
    assert bb.anchor_parse(proof) == (digest, [5, 870000], 1)


@pytest.mark.parametrize('data', [
    b'',
    b'not a proof',
    REAL_PENDING_PROOF[:-1],                      # truncated
    REAL_PENDING_PROOF + b'\x00',                 # trailing bytes
    HEADER + bytes(32) + b'\x42' + PENDING,       # unknown operation
    HEADER[:-1] + b'\x02' + bytes(20) + PENDING,  # a SHA1 proof: bb anchors SHA256 only
    HEADER + bytes(32) + b'\x00' + bb.ANCHOR_ATTESTATION_BITCOIN + b'\x01\xf0\x8c\x35',  # payload too short
])
def test_anchor_parse_refuses_what_is_not_a_proof(data):
    assert bb.anchor_parse(data) is None


def test_anchor_state_tells_pending_complete_and_stale(tmp_path):
    record = tmp_path / 'record.json'
    record.write_bytes(b"a\n")
    assert bb.anchor_state(record) == ('none', None)
    bb.anchor_proof_path(record).write_bytes(REAL_PENDING_PROOF)
    assert bb.anchor_state(record) == ('pending', None)
    bb.anchor_proof_path(record).write_bytes(HEADER + hashlib.sha256(b"a\n").digest() + bitcoin(b'\xf0\x8c\x35'))
    assert bb.anchor_state(record) == ('complete', 870000)
    assert bb.anchor_describe(record) == "Bitcoin block 870000"
    record.write_bytes(b"b\n")
    assert bb.anchor_state(record) == ('stale', None)
    bb.anchor_proof_path(record).write_bytes(b'garbage')
    assert bb.anchor_state(record) == ('unreadable', None)


def test_anchor_stamps_only_what_has_no_proof(mock_bb_dir, tmp_path, stand_in, capsys):
    _add(tmp_path)
    assert _states() == ['none'] * 3  # the object, its mapping and its lineage step
    bb.command_anchor()
    assert "Anchored: 3 files" in capsys.readouterr().out
    assert _states() == ['pending'] * 3
    proofs = {path: bb.anchor_proof_path(path).read_bytes() for path in bb.anchor_list_files()}

    bb.command_anchor()
    assert "Nothing to anchor." in capsys.readouterr().out
    _add(tmp_path, "def two():\n    return 2\n", 'two.py')
    bb.command_anchor()
    assert "Anchored: 3 files" in capsys.readouterr().out
    assert all(bb.anchor_proof_path(path).read_bytes() == proof for path, proof in proofs.items())


def test_anchor_one_function_only(mock_bb_dir, tmp_path, stand_in, capsys):
    _add(tmp_path)
    _add(tmp_path, "def two():\n    return 2\n", 'two.py')
    bb.command_anchor('one@eng')
    assert "Anchored: 3 files" in capsys.readouterr().out
    assert _states() == ['none'] * 3 + ['pending'] * 3


def test_anchor_upgrade_completes_and_keeps_one_proof_per_file(mock_bb_dir, tmp_path, stand_in, capsys):
    _add(tmp_path)
    bb.command_anchor()
    bb.command_anchor(upgrade=True)
    out = capsys.readouterr()
    assert "Completed: 3" in out.out and "Still pending: 0" in out.out and out.err == ""
    assert {bb.anchor_state(path) for path in bb.anchor_list_files()} == {('complete', 870000)}
    assert not list(bb.storage_get_bb_directory().rglob('*.bak'))

    bb.command_anchor(status=True)
    assert "Complete: 3 (Bitcoin block 870000)" in capsys.readouterr().out
    bb.command_log()
    assert capsys.readouterr().out.count("Anchored: Bitcoin block 870000") == 2  # the function and its step


def test_anchor_verify_relays_the_client(mock_bb_dir, tmp_path, stand_in, capsys, monkeypatch):
    _add(tmp_path)
    bb.command_anchor()
    bb.command_anchor(verify=True)
    assert capsys.readouterr().out.count(": pending") == 3
    bb.command_anchor(upgrade=True)
    bb.command_anchor(verify=True)
    assert capsys.readouterr().out.count("attests existence as of 2026-11-23") == 3

    monkeypatch.setenv('STAND_IN_REFUSES', '1')
    with pytest.raises(SystemExit) as failure:
        bb.command_anchor(verify=True)
    assert failure.value.code == 1
    assert "Could not connect to local Bitcoin node" in capsys.readouterr().out


def test_anchor_reports_a_file_changed_since(mock_bb_dir, tmp_path, stand_in, capsys):
    _add(tmp_path)
    bb.command_anchor()
    mapping = next(bb.storage_get_mappings_directory().rglob('*.json'))
    mapping.write_text(mapping.read_text(encoding='utf-8').replace('one', 'won'), encoding='utf-8')
    capsys.readouterr()
    for mode in ({'status': True}, {'verify': True}, {'upgrade': True}, {}):
        with pytest.raises(SystemExit) as failure:
            bb.command_anchor(**mode)
        assert failure.value.code == 1
        assert "the file changed since it was anchored" in capsys.readouterr().err
    # The changed file is not stamped again: its proof stays, and stays wrong
    assert bb.anchor_state(mapping) == ('stale', None)


def test_anchor_without_the_client_explains(mock_bb_dir, tmp_path, monkeypatch, capsys):
    _add(tmp_path)
    monkeypatch.setenv('BB_OTS', str(tmp_path / 'no-such-client'))
    with pytest.raises(SystemExit) as failure:
        bb.command_anchor()
    assert failure.value.code == 1
    assert "OpenTimestamps client not found" in capsys.readouterr().err
    assert _states() == ['none'] * 3
    bb.command_anchor(status=True)  # needs no client
    assert "Not anchored: 3" in capsys.readouterr().out


def test_anchor_client_failure_is_an_error(mock_bb_dir, tmp_path, monkeypatch, capsys):
    _add(tmp_path)
    script = tmp_path / 'failing.py'
    script.write_text("import sys\nsys.exit('no calendar answered')\n", encoding='utf-8')
    monkeypatch.setenv('BB_OTS', f"{sys.executable} {script}")
    with pytest.raises(SystemExit) as failure:
        bb.command_anchor()
    err = capsys.readouterr().err
    assert failure.value.code == 1
    assert "ots: no calendar answered" in err and "3 file(s) not anchored" in err


def test_rm_takes_the_proofs_along(mock_bb_dir, tmp_path, stand_in):
    _add(tmp_path)
    bb.command_anchor()
    bb.command_rm('one@eng')
    assert [path.name for path in bb.storage_get_pool_directory().rglob('*.ots')] == ['object.json.ots']
