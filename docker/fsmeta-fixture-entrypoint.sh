#!/usr/bin/env bash
set -euo pipefail

passwd_path="${FS_META_PASSWD_PATH:?FS_META_PASSWD_PATH is required}"
shadow_path="${FS_META_SHADOW_PATH:?FS_META_SHADOW_PATH is required}"
query_keys_path="${FS_META_QUERY_KEYS_PATH:?FS_META_QUERY_KEYS_PATH is required}"

mkdir -p "$(dirname "$passwd_path")" "$(dirname "$shadow_path")" "$(dirname "$query_keys_path")"

if [[ ! -f "$passwd_path" ]]; then
    printf 'admin:1000:1000:fsmeta_management:/home/admin:/bin/bash:0\n' >"$passwd_path"
fi

if [[ ! -f "$shadow_path" ]]; then
    printf 'admin:plain$admin:0\n' >"$shadow_path"
fi

if [[ ! -f "$query_keys_path" ]]; then
    printf '{\n  "keys": []\n}\n' >"$query_keys_path"
fi

exec /usr/local/bin/fs_meta_api_fixture
