#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
from pathlib import Path
import re
import sys

root = Path('documentation/content')
errors = []
allowed_tutorial_admin = {
    'admin-bootstrap.md',
    'setup-local-anvil.md',
    'mesh-regions-cells-and-nodes.md',
    'mesh-routing-and-lifecycle.md',
    'repair-and-diagnostics.md',
    'tenants-apps-and-credentials.md',
}

def first_command_group(line: str):
    stripped = line.strip()
    if stripped.startswith('anvil-admin '):
        tail = stripped.removeprefix('anvil-admin').strip()
    elif re.search(r'\banvil-local\s+\\?$', stripped):
        # Multi-line docker exec command; the next line carries the real command.
        return None
    elif re.search(r'\banvil-admin\b', stripped) and re.match(r'^(docker|podman)\s+exec\b', stripped):
        tail = stripped.split('anvil-admin', 1)[1].strip()
    else:
        return None
    if not tail:
        return None
    tokens = tail.split()
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token.startswith('--'):
            i += 2
            continue
        return token
    return None

for path in (root / 'tutorials').glob('*.md'):
    if path.name in allowed_tutorial_admin:
        continue
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        group = first_command_group(line)
        if group is not None:
            errors.append(f'{path}:{lineno}: tenant tutorial uses anvil-admin {group}')

pseudo_patterns = [
    (re.compile(r'AcquireTaskLeaseRequest[\s\S]{0,400}\bttl_ms\b'), 'ttl_ms is not a task lease proto field; use requested_ttl_nanos'),
    (re.compile(r'\banvil-admin\s+admin\b'), 'nested anvil-admin admin command is not valid'),
    (re.compile(r'\bANVIL_BOOTSTRAP_ADMIN_TOKEN\b'), 'bootstrap admin token bypass must not be documented'),
    (re.compile(r'\bbootstrap token\b', re.IGNORECASE), 'bootstrap token bypass language must not be documented'),
    (re.compile(r'\bbootstrap bearer[- ]token\b', re.IGNORECASE), 'bootstrap bearer token bypass language must not be documented'),
    (re.compile(r'\bAnvilAdminCapability\b'), 'admin capabilities must be documented as system-realm Zanzibar relations'),
    (re.compile(r'\banvil_admin capabilities\b', re.IGNORECASE), 'admin capabilities must be documented as system-realm Zanzibar relations'),
    (re.compile(r'local CLI that writes to local Anvil storage', re.IGNORECASE), 'admin CLI must not document direct local storage writes'),
    (re.compile(r'\bWorka\b|worka-', re.IGNORECASE), 'Anvil docs must not mention Worka'),
]
for path in list(root.rglob('*.md')) + list(Path('docs').rglob('*.md')):
    text = path.read_text()
    for pattern, message in pseudo_patterns:
        checked_text = text
        if message == 'Anvil docs must not mention Worka':
            # Release metadata must name the canonical container repository.
            # Keep the branding prohibition everywhere else.
            checked_text = checked_text.replace('ghcr.io/worka-ai/anvil', '')
        if pattern.search(checked_text):
            errors.append(f'{path}: {message}')

command_families = {
    'anvil': {
        ':','-server','...','<command>',
        'static-config','configure','bucket','object','auth','authz','audit','app','index','watch','personaldb','stream','lease','diagnostics','repair','host-alias','hf-key','hf-ingest'
    },
    'anvil-admin': {
        'key','tenant','app','policy','secret-encryption-key','personal-db-signing-key','bucket','region','cell','node','routing','repair','diagnostics','audit','host-alias'
    },
}
command_line = re.compile(r'^\s*(anvil-admin|anvil)\b(.*)$')
for path in root.rglob('*.md'):
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        m = command_line.match(line)
        if not m:
            continue
        binary, tail = m.group(1), m.group(2).strip()
        tokens = tail.split()
        group = None
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token.startswith('--'):
                i += 2
                continue
            group = token
            break
        if group and group not in command_families[binary] and not group.startswith('$') and not group.startswith('"'):
            errors.append(f'{path}:{lineno}: unknown {binary} command group {group!r}')

if errors:
    print('Documentation hardening check failed:', file=sys.stderr)
    for error in errors:
        print(f'  - {error}', file=sys.stderr)
    sys.exit(1)
PY
