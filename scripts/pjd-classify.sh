#!/bin/bash
# Classify pjdfstest failures from verbose prove logs (e2e-work/pjdv-<area>.log) into buckets:
#   perm-matrix : subtest switches uid/gid (-u/-g) — by design inapplicable under identity squash
#   chown       : op is chown/lchown — squash ignores ownership changes by design
#   notsupp     : hard links / mknod-family — procedures not implemented (NOTSUPP)
#   nametoolong : ENAMETOOLONG expectations
#   other       : everything else — the real-signal bucket, listed individually
set -euo pipefail
cd "$(dirname "$0")/.."

total_fail=0
for log in e2e-work/pjdv-*.log; do
  area=$(basename "$log" .log | sed 's/^pjdv-//')
  perm=0; chown=0; notsupp=0; ntl=0; other=0
  others=""
  while IFS= read -r line; do
    case "$line" in
      *"tried '"*"-u "*|*"tried '"*"-g "*) perm=$((perm+1)) ;;
      *"tried 'chown"*|*"tried 'lchown"*) chown=$((chown+1)) ;;
      *"tried 'link"*|*"tried 'mkfifo"*|*"tried 'mknod"*|*"tried 'bind"*|*"got EOPNOTSUPP"*|*"got ENOTSUP"*) notsupp=$((notsupp+1)) ;;
      *"expected ENAMETOOLONG"*) ntl=$((ntl+1)) ;;
      *) other=$((other+1)); others="$others
    $line" ;;
    esac
  done < <(grep -a "^not ok" "$log" | grep -a "tried")
  f=$((perm+chown+notsupp+ntl+other)); total_fail=$((total_fail+f))
  printf '%-10s fail=%-5s perm=%-5s chown=%-4s notsupp=%-4s nametoolong=%-3s OTHER=%s\n' \
    "$area" "$f" "$perm" "$chown" "$notsupp" "$ntl" "$other"
  if [ "$other" -gt 0 ] && [ "${VERBOSE:-0}" = 1 ]; then echo "$others" | head -40; fi
done
echo "total classified failures: $total_fail"
