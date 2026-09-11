#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail() { echo "$*" >&2; exit 1; }

platform_values() {
  case "$1" in
    linux/amd64)
      target=x86_64-unknown-linux-gnu; cargo_subcommand=zigbuild
      cargo_target=x86_64-unknown-linux-gnu.2.17; build_mode=prebuilt-zigbuild; compiler='cargo zigbuild'
      machine='Advanced Micro Devices X86-64'; interpreter=/lib64/ld-linux-x86-64.so.2; glibc_ceiling=2.17 ;;
    linux/arm64)
      target=aarch64-unknown-linux-gnu; cargo_subcommand=build
      cargo_target="$target"; build_mode=prebuilt-native; compiler='cargo build'
      machine=AArch64; interpreter=/lib/ld-linux-aarch64.so.1; glibc_ceiling=2.41 ;;
    *) fail "unsupported release platform $1" ;;
  esac
}

elf_metadata() {
  local binary="$1" output="$2" actual_machine actual_interpreter glibc_max needed elf_type
  [[ "$(readelf -h "$binary" | sed -n 's/^  Class:[[:space:]]*//p')" == ELF64 ]] || fail "$binary is not ELF64"
  [[ "$(readelf -h "$binary" | sed -n 's/^  Data:[[:space:]]*//p')" == "2's complement, little endian" ]] || fail "$binary is not little-endian ELF"
  [[ "$(readelf -h "$binary" | sed -n 's,^  OS/ABI:[[:space:]]*,,p')" == "UNIX - System V" ]] || fail "$binary does not use the GNU/System-V ABI"
  elf_type="$(readelf -h "$binary" | sed -n 's/^  Type:[[:space:]]*\([^ ]*\).*/\1/p')"
  [[ "$elf_type" == DYN || "$elf_type" == EXEC ]] || fail "$binary is not an executable ELF"
  actual_machine="$(readelf -h "$binary" | sed -n 's/^  Machine:[[:space:]]*//p')"
  [[ "$actual_machine" == "$machine" ]] || fail "$binary machine $actual_machine does not match $machine"
  actual_interpreter="$(readelf -lW "$binary" | sed -n 's/.*Requesting program interpreter: \([^]]*\)].*/\1/p')"
  [[ "$actual_interpreter" == "$interpreter" ]] || fail "$binary interpreter $actual_interpreter does not match $interpreter"
  needed="$(readelf -dW "$binary" | sed -n 's/.*Shared library: \[\([^]]*\)\].*/\1/p' | sort -u)"
  [[ -n "$needed" ]] || fail "$binary has no dynamic GNU runtime dependencies"
  if grep -Ev '^(ld-linux-(x86-64|aarch64)\.so([.][0-9]+)*|lib(c|m|dl|rt|pthread|resolv|util|gcc_s|stdc\+\+|ssl|crypto|z|zstd|lz4|snappy|bz2)\.so([.][0-9]+)*)$' <<<"$needed"; then
    fail "$binary has an unexpected dynamic dependency"
  fi
  glibc_max="$(readelf --version-info "$binary" | grep -oE 'GLIBC_[0-9]+\.[0-9]+' | sed 's/GLIBC_//' | sort -Vu | tail -n1)"
  [[ -n "$glibc_max" ]] || fail "$binary does not declare a glibc ABI"
  [[ "$(printf '%s\n%s\n' "$glibc_max" "$glibc_ceiling" | sort -V | tail -n1)" == "$glibc_ceiling" ]] || fail "$binary requires glibc $glibc_max, above $glibc_ceiling"
  jq -n --arg machine "$actual_machine" --arg interpreter "$actual_interpreter" \
    --arg glibc_max "$glibc_max" --arg needed "$needed" \
    --arg elf_type "$elf_type" \
    '{format:"elf64",endianness:"little",abi:"gnu-system-v",type:$elf_type,machine:$machine,interpreter:$interpreter,glibc_max:$glibc_max,needed:($needed|split("\n"))}' >"$output"
}

build_inputs() {
  [[ "$#" == 4 ]] || fail "usage: $0 build <version> <platform> <source-commit> <output-dir>"
  local version="$1" platform="$2" commit="$3" output_dir="$4"
  platform_values "$platform"
  [[ "${KELDRA_ZRUNNER_JOB_ID:-}" =~ ^[0-9A-HJKMNP-TV-Z]{26}$ ]] || fail "KELDRA_ZRUNNER_JOB_ID must bind this zrunner job"
  [[ "${RUSTUP_TOOLCHAIN:-}" == 1.96.0 ]] || fail "release build requires RUSTUP_TOOLCHAIN=1.96.0"
  [[ "${CARGO_PROFILE_DEV_CODEGEN_BACKEND:-}" == llvm ]] || fail "release build requires zrunner's LLVM backend evidence"
  [[ "${CARGO_TARGET_DIR:-}" == /home/zcourts/projects/projects/build/debian1/keldra ]] || fail "unexpected CARGO_TARGET_DIR"
  [[ "$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')" == "$commit" ]] || fail "source checkout is not $commit"
  [[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=normal)" ]] || fail "release build requires a clean checkout"
  local workspace_version jobs
  workspace_version="$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\([^"]*\)"/\1/p' "$repo_root/Cargo.toml")"
  [[ "$workspace_version" == "$version" ]] || fail "workspace version $workspace_version does not match $version"
  jobs="${CARGO_BUILD_JOBS:-1}"; [[ "$jobs" =~ ^[12]$ ]] || fail "release build requires one or two zrunner-assigned jobs"
  local -a command=(cargo "$cargo_subcommand" --locked --release --jobs "$jobs" --target "$cargo_target" -p keldra-server --bin keldra-server -p keldra-cli --bin keldra)
  (cd "$repo_root" && "${command[@]}")
  [[ "$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')" == "$commit" ]] || fail "source checkout changed during the release build"
  [[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=normal)" ]] || fail "source tree changed during the release build"
  local artifact_dir="$CARGO_TARGET_DIR/$target/release" scratch rustc_version rustc_commit
  scratch="$(mktemp -d "${output_dir}.tmp.XXXXXX")"; trap 'rm -rf -- "$scratch"' EXIT
  for binary in keldra-server keldra; do
    [[ -x "$artifact_dir/$binary" ]] || fail "build did not produce $artifact_dir/$binary"
    install -m 0555 "$artifact_dir/$binary" "$scratch/$binary"
    elf_metadata "$scratch/$binary" "$scratch/$binary.elf.json"
  done
  rustc_version="$(rustc --version --verbose | sed -n '1p')"
  rustc_commit="$(rustc --version --verbose | sed -n 's/^commit-hash: //p')"
  jq -n --arg version "$version" --arg source_commit "$commit" --arg platform "$platform" \
    --arg target "$target" --arg cargo_target "$cargo_target" --arg build_mode "$build_mode" --arg compiler "$compiler" \
    --arg job_id "$KELDRA_ZRUNNER_JOB_ID" --arg rustc_version "$rustc_version" --arg rustc_commit "$rustc_commit" \
    --argjson command "$(printf '%s\n' "${command[@]}" | jq -R . | jq -s .)" \
    --slurpfile server_elf "$scratch/keldra-server.elf.json" --slurpfile cli_elf "$scratch/keldra.elf.json" \
    --arg server_sha256 "sha256:$(sha256sum "$scratch/keldra-server" | awk '{print $1}')" \
    --arg cli_sha256 "sha256:$(sha256sum "$scratch/keldra" | awk '{print $1}')" \
    '{schema:"keldra.zrunner-release-build.v1",version:$version,source_commit:$source_commit,platform:$platform,target:$target,cargo_target:$cargo_target,build_mode:$build_mode,compiler:$compiler,zrunner_job_id:$job_id,command:$command,toolchain:{rustc:$rustc_version,rustc_commit:$rustc_commit},binaries:{"keldra-server":{sha256:$server_sha256,elf:$server_elf[0]},keldra:{sha256:$cli_sha256,elf:$cli_elf[0]}}}' \
    >"$scratch/build-manifest.json"
  rm "$scratch"/*.elf.json
  [[ ! -e "$output_dir" ]] || fail "refusing to replace existing build evidence $output_dir"
  mv "$scratch" "$output_dir"; trap - EXIT
}

seal_inputs() {
  [[ "$#" == 4 ]] || fail "usage: $0 seal <build-dir> <zrunner-job-json> <zrunner-completion-json> <output-dir>"
  local build_dir="$1" job_json="$2" completion_json="$3" output_dir="$4"
  local manifest="$build_dir/build-manifest.json" platform version commit job_id scratch_elf
  platform="$(jq -er '.platform' "$manifest")"; platform_values "$platform"
  version="$(jq -er '.version' "$manifest")"; commit="$(jq -er '.source_commit' "$manifest")"; job_id="$(jq -er '.zrunner_job_id' "$manifest")"
  jq -e --arg target "$target" --arg cargo_target "$cargo_target" --arg mode "$build_mode" --arg compiler "$compiler" --arg subcommand "$cargo_subcommand" '
    .schema == "keldra.zrunner-release-build.v1" and .target == $target and .cargo_target == $cargo_target and .build_mode == $mode and .compiler == $compiler and
    (.toolchain.rustc | startswith("rustc 1.96.0 ")) and (.toolchain.rustc_commit | test("^[0-9a-f]{40}$")) and
    (.command == ["cargo",$subcommand,"--locked","--release","--jobs",.command[5],"--target",$cargo_target,"-p","keldra-server","--bin","keldra-server","-p","keldra-cli","--bin","keldra"]) and
    (.command[5] == "1" or .command[5] == "2")
  ' "$manifest" >/dev/null || fail "build manifest command, target, or toolchain is invalid"
  jq -e --arg id "$job_id" --arg cwd "$repo_root" --arg version "$version" --arg platform "$platform" --arg commit "$commit" --arg build_dir "$build_dir" '
    .schema == "zrunner.job.v1" and .id == $id and .runner == "debian1" and .profile == "rust" and .cwd == $cwd and
    .env.RUSTUP_TOOLCHAIN == "1.96.0" and .env.CARGO_PROFILE_DEV_CODEGEN_BACKEND == "llvm" and .env.KELDRA_ZRUNNER_JOB_ID == $id and .env.CARGO_TARGET_DIR == "/home/zcourts/projects/projects/build/debian1/keldra" and
    (.locks | index("cargo-target:debian1:keldra") != null) and
    (.argv == ["./scripts/prepare-release-image-input.sh","build",$version,$platform,$commit,$build_dir])
  ' "$job_json" >/dev/null || fail "zrunner job does not authorize this exact release build"
  jq -e --arg id "$job_id" '.schema == "zrunner.event.v1" and .job == $id and .runner == "debian1" and .state == "completed" and (.detail | test("^elapsed_ms=[0-9]+( exit_code=0)?$"))' "$completion_json" >/dev/null || fail "zrunner completion does not prove success"
  for binary in keldra-server keldra; do
    [[ "sha256:$(sha256sum "$build_dir/$binary" | awk '{print $1}')" == "$(jq -er --arg binary "$binary" '.binaries[$binary].sha256' "$manifest")" ]] || fail "$binary changed after build"
    scratch_elf="$(mktemp)"; elf_metadata "$build_dir/$binary" "$scratch_elf"
    jq -e --arg binary "$binary" --slurpfile elf "$scratch_elf" '.binaries[$binary].elf == $elf[0]' "$manifest" >/dev/null || fail "$binary ELF metadata changed"
    rm "$scratch_elf"
  done
  [[ ! -e "$output_dir" ]] || fail "refusing to replace existing sealed input $output_dir"
  mkdir "$output_dir"; install -m 0555 "$build_dir/keldra-server" "$output_dir/keldra-server"; install -m 0555 "$build_dir/keldra" "$output_dir/keldra"
  jq --arg job_sha256 "sha256:$(sha256sum "$job_json" | awk '{print $1}')" --arg completion_sha256 "sha256:$(sha256sum "$completion_json" | awk '{print $1}')" \
    --arg manifest_sha256 "sha256:$(sha256sum "$manifest" | awk '{print $1}')" \
    '. + {schema:"keldra.release-image-input.v1",evidence:{job_sha256:$job_sha256,completion_sha256:$completion_sha256,build_manifest_sha256:$manifest_sha256}}' "$manifest" >"$output_dir/input.json"
}

qualify_image() {
  [[ "$#" == 5 ]] || fail "usage: $0 qualify <version> <commit> <image-record> <oci-archive> <output>"
  local version="$1" commit="$2" image_record="$3" archive="$4" output="$5" platform expected_archive image
  [[ "${KELDRA_ZRUNNER_JOB_ID:-}" =~ ^[0-9A-HJKMNP-TV-Z]{26}$ ]] || fail "KELDRA_ZRUNNER_JOB_ID must bind this zrunner job"
  [[ "${RUSTUP_TOOLCHAIN:-}" == 1.96.0 ]] || fail "qualification requires RUSTUP_TOOLCHAIN=1.96.0"
  [[ "${CARGO_PROFILE_DEV_CODEGEN_BACKEND:-}" == llvm ]] || fail "qualification requires zrunner's LLVM backend evidence"
  [[ "${CARGO_TARGET_DIR:-}" == /home/zcourts/projects/projects/build/*/keldra ]] || fail "qualification Cargo target changed"
  platform="$(jq -er '.platform' "$image_record")"; platform_values "$platform"
  case "$platform:$(uname -m)" in linux/amd64:x86_64|linux/arm64:aarch64|linux/arm64:arm64) ;; *) fail "$platform release qualification requires a matching native host" ;; esac
  "$repo_root/scripts/release-record.py" verify-image --record "$image_record" --version "$version" --commit "$commit" --platform "$platform"
  expected_archive="$(jq -er '.oci_archive_sha256' "$image_record")"
  [[ "sha256:$(sha256sum "$archive" | awk '{print $1}')" == "$expected_archive" ]] || fail "qualification archive bytes changed"
  image="keldra:qualification-${KELDRA_ZRUNNER_JOB_ID,,}"
  skopeo copy --override-os linux --override-arch "${platform#linux/}" \
    "oci-archive:${archive}" "docker-daemon:${image}"
  KELDRA_IMAGE="$image" KELDRA_DOCKER_PLATFORM="$platform" KELDRA_QUALIFICATION_MODE=release \
    KELDRA_QUALIFICATION_VERSION="$version" "$repo_root/scripts/qualify-three-node.sh"
  [[ "$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}')" == "$commit" ]] || fail "source checkout changed during three-node qualification"
  [[ -z "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=normal)" ]] || fail "source tree changed during three-node qualification"
  jq -n --arg version "$version" --arg source_commit "$commit" --arg job_id "$KELDRA_ZRUNNER_JOB_ID" \
    --arg image_record_sha256 "sha256:$(sha256sum "$image_record" | awk '{print $1}')" \
    --arg oci_archive_sha256 "$expected_archive" --arg oci_index_digest "$(jq -er '.oci_index_digest' "$image_record")" \
    --arg runnable_manifest_digest "$(jq -er '.runnable_manifest_digest' "$image_record")" --arg platform "$platform" --arg host_architecture "$(uname -m)" \
    '{schema:"keldra.local-three-node-run.v1",result:"pass",platform:$platform,host_architecture:$host_architecture,version:$version,source_commit:$source_commit,zrunner_job_id:$job_id,image_record_sha256:$image_record_sha256,oci_archive_sha256:$oci_archive_sha256,oci_index_digest:$oci_index_digest,runnable_manifest_digest:$runnable_manifest_digest}' >"$output"
}

case "${1:-}" in
  build) shift; build_inputs "$@" ;; seal) shift; seal_inputs "$@" ;; qualify) shift; qualify_image "$@" ;;
  *) fail "usage: $0 <build|seal|qualify> ..." ;;
esac
