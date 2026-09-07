#!/usr/bin/env python3
"""Create and verify the immutable record which authorizes a Keldra release."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
from typing import Any


SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z._-]+)?$")
TARGETS = {
    "linux/amd64": ("x86_64-unknown-linux-gnu", "x86-64"),
    "linux/arm64": ("aarch64-unknown-linux-gnu", "aarch64"),
}
ACCEPTED_INDEX_GATES = ["index-v1-sustained", "catalog-250k"]
INDEX_SCALE_SHARDS = 16


def fail(message: str) -> None:
    raise SystemExit(message)


def read_json(path: str) -> dict[str, Any]:
    try:
        value = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read release evidence {path}: {error}")
    if not isinstance(value, dict):
        fail(f"release evidence {path} must contain one JSON object")
    return value


def write_json(path: str, value: dict[str, Any]) -> None:
    destination = pathlib.Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def validate_identity(value: dict[str, Any], version: str, commit: str, label: str) -> None:
    require(value.get("version") == version, f"{label} version does not match {version}")
    require(value.get("source_commit") == commit, f"{label} source commit does not match {commit}")


def validate_image(value: dict[str, Any], version: str, commit: str) -> None:
    require(value.get("schema") == "keldra.release-image.v1", "invalid release-image schema")
    validate_identity(value, version, commit, "release image")
    platform = value.get("platform")
    require(platform in TARGETS, f"unsupported release-image platform {platform!r}")
    target, architecture = TARGETS[platform]
    require(value.get("target") == target, f"release-image target does not match {platform}")
    require(value.get("architecture") == architecture, f"release-image architecture does not match {platform}")
    for field in ("oci_archive_sha256", "oci_index_digest", "runnable_manifest_digest"):
        require(bool(SHA256.fullmatch(str(value.get(field, "")))), f"invalid release-image {field}")
    inputs = value.get("build_inputs")
    require(isinstance(inputs, dict), "release image build inputs are missing")
    require(bool(re.fullmatch(r"rust:1\.96-trixie@sha256:[0-9a-f]{64}", str(inputs.get("builder_image", "")))), "builder image is not digest-pinned")
    require(bool(re.fullmatch(r"debian:trixie-slim@sha256:[0-9a-f]{64}", str(inputs.get("runtime_image", "")))), "runtime image is not digest-pinned")
    require(inputs.get("rust_toolchain") == "1.96.0", "release image Rust toolchain is not 1.96.0")
    binaries = value.get("binaries")
    require(isinstance(binaries, dict), "release image binaries are missing")
    for name in ("keldra-server", "keldra"):
        binary = binaries.get(name) if isinstance(binaries, dict) else None
        require(isinstance(binary, dict), f"release image is missing {name}")
        require(bool(SHA256.fullmatch(str(binary.get("sha256", "")))), f"invalid {name} SHA-256")
        require(binary.get("architecture") == architecture, f"{name} architecture does not match image")


def validate_three_node(value: dict[str, Any], version: str, commit: str, amd64: dict[str, Any]) -> None:
    require(value.get("schema") == "keldra.qualification-result.v1", "invalid three-node result schema")
    require(value.get("gate_id") == "three-node-release", "three-node result has the wrong gate ID")
    require(value.get("result") == "pass", "three-node release qualification did not pass")
    validate_identity(value, version, commit, "three-node result")
    require(value.get("platform") == "linux/amd64", "three-node result must qualify linux/amd64")
    require(value.get("oci_index_digest") == amd64["oci_index_digest"], "three-node result qualified a different OCI index")
    require(value.get("runnable_manifest_digest") == amd64["runnable_manifest_digest"], "three-node result qualified a different runnable manifest")
    require(str(value.get("run_id", "")).isdigit(), "three-node result is missing its run ID")
    require(str(value.get("run_attempt", "")).isdigit(), "three-node result is missing its run attempt")


def expected_index_runners() -> dict[str, str]:
    scripts = pathlib.Path(__file__).resolve().parent
    return {
        "index_v1_sustained": sha256_file(str(scripts / "qualify-index-v1-ssd-scale.sh")),
        "catalog_250k": sha256_file(str(scripts / "qualify-index-catalog.sh")),
    }


def validate_index_part(
    part: dict[str, Any], version: str, commit: str, storage_class: str
) -> None:
    require(part.get("schema") == "keldra.v1-index.part-result.v1", "invalid v1-index part schema")
    validate_identity(part, version, commit, f"{storage_class} index part")
    require(part.get("storage_class") == storage_class, "v1-index part storage class changed")
    require(part.get("result") == "pass", "v1-index part did not pass")
    require(part.get("gate_id") in ACCEPTED_INDEX_GATES, "v1-index part has an unknown gate")
    require(isinstance(part.get("run_id"), str) and bool(part["run_id"]), "v1-index part is missing its run ID")
    for field in ("evidence_bundle_sha256", "hardware_fingerprint_sha256", "kit_manifest_sha256"):
        require(bool(SHA256.fullmatch(str(part.get(field, "")))), f"invalid v1-index part {field}")
    require(part.get("runner_sha256") == expected_index_runners(), "v1-index part used different runners")
    if part["gate_id"] == "index-v1-sustained":
        require(part.get("shard_count") == INDEX_SCALE_SHARDS, "v1-index scale shard count changed")
        require(isinstance(part.get("shard_index"), int), "v1-index scale shard index is missing")
        require(0 <= part["shard_index"] < INDEX_SCALE_SHARDS, "v1-index scale shard index is invalid")
    else:
        require(part.get("shard_index") is None and part.get("shard_count") is None, "catalog part must not be sharded")


def validate_index(value: dict[str, Any], version: str, commit: str) -> None:
    require(value.get("schema") == "keldra.v1-index.acceptance.v1", "invalid accepted v1-index evidence schema")
    require(value.get("gate_id") == "accepted-v1-index", "index evidence is not the accepted v1-index gate")
    require(value.get("result") == "pass", "accepted v1-index qualification did not pass")
    validate_identity(value, version, commit, "accepted v1-index evidence")
    require(value.get("format") == "v1", "accepted index evidence must cover only format v1")
    require(value.get("storage_classes") == ["ssd", "rotational"], "accepted index evidence must cover SSD and rotational storage")
    require(value.get("gates") == ACCEPTED_INDEX_GATES, "accepted v1-index gate set changed or is incomplete")
    require(bool(SHA256.fullmatch(str(value.get("evidence_bundle_sha256", "")))), "invalid v1-index evidence bundle SHA-256")
    runner_hashes = value.get("runner_sha256")
    require(isinstance(runner_hashes, dict), "accepted v1-index runner hashes are missing")
    expected_runners = expected_index_runners()
    require(runner_hashes == expected_runners, "accepted v1-index evidence used different runners")
    runs = value.get("runs")
    require(isinstance(runs, list) and len(runs) == 2, "accepted v1-index evidence requires exactly two storage runs")
    combined = hashlib.sha256()
    for run, storage_class in zip(runs, ("ssd", "rotational"), strict=True):
        require(isinstance(run, dict), "accepted v1-index storage result must be an object")
        require(run.get("schema") == "keldra.v1-index.host-result.v1", f"invalid {storage_class} host-result schema")
        validate_identity(run, version, commit, f"{storage_class} host result")
        require(run.get("storage_class") == storage_class, f"missing exact {storage_class} index result")
        require(run.get("result") == "pass", f"{storage_class} index qualification did not pass")
        require(isinstance(run.get("run_id"), str) and bool(run["run_id"]), f"{storage_class} result is missing its run ID")
        require(bool(SHA256.fullmatch(str(run.get("evidence_bundle_sha256", "")))), f"invalid {storage_class} evidence SHA-256")
        require(bool(SHA256.fullmatch(str(run.get("hardware_fingerprint_sha256", "")))), f"invalid {storage_class} hardware fingerprint SHA-256")
        require(bool(SHA256.fullmatch(str(run.get("kit_manifest_sha256", "")))), f"invalid {storage_class} kit manifest SHA-256")
        require(run.get("gates") == ACCEPTED_INDEX_GATES, f"{storage_class} host did not run the accepted gates")
        require(run.get("runner_sha256") == expected_runners, f"{storage_class} host used different runners")
        parts = run.get("parts")
        require(isinstance(parts, list) and len(parts) == INDEX_SCALE_SHARDS + 1, f"{storage_class} host part set is incomplete")
        for part in parts:
            require(isinstance(part, dict), "v1-index part must be an object")
            validate_index_part(part, version, commit, storage_class)
        scale_parts = [part for part in parts if part["gate_id"] == "index-v1-sustained"]
        catalog_parts = [part for part in parts if part["gate_id"] == "catalog-250k"]
        require(len(catalog_parts) == 1, f"{storage_class} requires exactly one catalog part")
        require(
            sorted(part["shard_index"] for part in scale_parts) == list(range(INDEX_SCALE_SHARDS)),
            f"{storage_class} sustained shard set is incomplete",
        )
        ordered_parts = sorted(scale_parts, key=lambda part: part["shard_index"]) + catalog_parts
        require({part["kit_manifest_sha256"] for part in ordered_parts} == {run["kit_manifest_sha256"]}, f"{storage_class} parts used different candidate kits")
        evidence = hashlib.sha256()
        hardware = hashlib.sha256()
        for part in ordered_parts:
            evidence.update(part["evidence_bundle_sha256"].encode("ascii"))
            hardware.update(part["hardware_fingerprint_sha256"].encode("ascii"))
        require(run["evidence_bundle_sha256"] == "sha256:" + evidence.hexdigest(), f"{storage_class} evidence aggregate changed")
        require(run["hardware_fingerprint_sha256"] == "sha256:" + hardware.hexdigest(), f"{storage_class} hardware aggregate changed")
        combined.update(run["evidence_bundle_sha256"].encode("ascii"))
    require(len({run["kit_manifest_sha256"] for run in runs}) == 1, "SSD and rotational runs used different candidate kits")
    require(value.get("evidence_bundle_sha256") == "sha256:" + combined.hexdigest(), "accepted v1-index aggregate evidence identity changed")


def accept_index_command(args: argparse.Namespace) -> None:
    require(bool(VERSION.fullmatch(args.version)), "invalid release version")
    require(bool(COMMIT.fullmatch(args.commit)), "invalid source commit")
    parts = [read_json(path) for path in args.part]
    require(len(parts) == 2 * (INDEX_SCALE_SHARDS + 1), "acceptance requires every SSD and rotational part")
    by_storage: dict[str, list[dict[str, Any]]] = {"ssd": [], "rotational": []}
    for part in parts:
        storage = part.get("storage_class")
        require(storage in by_storage, "invalid v1-index part storage class")
        validate_index_part(part, args.version, args.commit, storage)
        by_storage[storage].append(part)
    runs: list[dict[str, Any]] = []
    for storage in ("ssd", "rotational"):
        storage_parts = by_storage[storage]
        scale_parts = sorted(
            (part for part in storage_parts if part["gate_id"] == "index-v1-sustained"),
            key=lambda part: part["shard_index"],
        )
        catalog_parts = [part for part in storage_parts if part["gate_id"] == "catalog-250k"]
        require([part["shard_index"] for part in scale_parts] == list(range(INDEX_SCALE_SHARDS)), f"{storage} sustained shard set is incomplete")
        require(len(catalog_parts) == 1, f"{storage} requires exactly one catalog part")
        ordered = scale_parts + catalog_parts
        kit_hashes = {part["kit_manifest_sha256"] for part in ordered}
        require(len(kit_hashes) == 1, f"{storage} parts used different candidate kits")
        evidence = hashlib.sha256()
        hardware = hashlib.sha256()
        for part in ordered:
            evidence.update(part["evidence_bundle_sha256"].encode("ascii"))
            hardware.update(part["hardware_fingerprint_sha256"].encode("ascii"))
        runs.append({
            "schema": "keldra.v1-index.host-result.v1",
            "storage_class": storage,
            "result": "pass",
            "version": args.version,
            "source_commit": args.commit,
            "run_id": "+".join(part["run_id"] for part in ordered),
            "evidence_bundle_sha256": "sha256:" + evidence.hexdigest(),
            "hardware_fingerprint_sha256": "sha256:" + hardware.hexdigest(),
            "kit_manifest_sha256": next(iter(kit_hashes)),
            "runner_sha256": expected_index_runners(),
            "gates": ACCEPTED_INDEX_GATES,
            "parts": ordered,
        })
    combined = hashlib.sha256()
    for run in runs:
        combined.update(run["evidence_bundle_sha256"].encode("ascii"))
    require(len({run["kit_manifest_sha256"] for run in runs}) == 1, "SSD and rotational runs used different candidate kits")
    value = {
        "schema": "keldra.v1-index.acceptance.v1",
        "gate_id": "accepted-v1-index",
        "result": "pass",
        "format": "v1",
        "version": args.version,
        "source_commit": args.commit,
        "storage_classes": ["ssd", "rotational"],
        "gates": ACCEPTED_INDEX_GATES,
        "runner_sha256": runs[0]["runner_sha256"],
        "evidence_bundle_sha256": "sha256:" + combined.hexdigest(),
        "runs": runs,
    }
    validate_index(value, args.version, args.commit)
    write_json(args.output, value)


def image_command(args: argparse.Namespace) -> None:
    require(args.platform in TARGETS, f"unsupported platform {args.platform}")
    require(bool(VERSION.fullmatch(args.version)), "invalid release version")
    require(bool(COMMIT.fullmatch(args.commit)), "invalid source commit")
    require(bool(SHA256.fullmatch(args.oci_index_digest)), "invalid OCI index digest")
    require(bool(SHA256.fullmatch(args.runnable_manifest_digest)), "invalid runnable manifest digest")
    target, architecture = TARGETS[args.platform]
    value = {
        "schema": "keldra.release-image.v1",
        "version": args.version,
        "source_commit": args.commit,
        "target": target,
        "platform": args.platform,
        "architecture": architecture,
        "oci_archive_sha256": sha256_file(args.oci_archive),
        "oci_index_digest": args.oci_index_digest,
        "runnable_manifest_digest": args.runnable_manifest_digest,
        "build_inputs": {
            "builder_image": args.builder_image,
            "runtime_image": args.runtime_image,
            "rust_toolchain": "1.96.0",
        },
        "binaries": {
            "keldra-server": {"sha256": sha256_file(args.server_binary), "architecture": architecture},
            "keldra": {"sha256": sha256_file(args.cli_binary), "architecture": architecture},
        },
    }
    validate_image(value, args.version, args.commit)
    write_json(args.output, value)


def assemble_command(args: argparse.Namespace) -> None:
    require(bool(VERSION.fullmatch(args.version)), "invalid release version")
    require(bool(COMMIT.fullmatch(args.commit)), "invalid source commit")
    images = [read_json(path) for path in args.image]
    for item in images:
        validate_image(item, args.version, args.commit)
    by_platform = {item["platform"]: item for item in images}
    require(set(by_platform) == set(TARGETS), "release record requires exactly linux/amd64 and linux/arm64")
    require(len(by_platform) == len(images), "release record contains a duplicate platform")
    three_node = read_json(args.three_node)
    validate_three_node(three_node, args.version, args.commit, by_platform["linux/amd64"])
    prerelease = bool(re.search(r"[.-][0-9A-Za-z]", args.version.split(".", 2)[2]))
    package_paths: dict[str, str] = {}
    for specification in args.package_crate:
        name, separator, path = specification.partition("=")
        require(separator == "=" and name and path, "package crate must use name=path")
        require(name not in package_paths, f"duplicate package crate {name}")
        package_paths[name] = path
    require(set(package_paths) == {"keldra-api", "keldra"}, "release record requires both package crates")
    value = {
        "schema": "keldra.release-record.v1",
        "version": args.version,
        "source_commit": args.commit,
        "release": {"prerelease": prerelease, "make_latest": not prerelease},
        "images": [by_platform[platform] for platform in sorted(by_platform)],
        "qualification": {"three_node": three_node},
        "packages": [
            {"name": "keldra-api", "version": args.version, "publication_order": 1, "crate_sha256": sha256_file(package_paths["keldra-api"])},
            {"name": "keldra", "version": args.version, "publication_order": 2, "requires": f"keldra-api ={args.version}", "crate_sha256": sha256_file(package_paths["keldra"])},
        ],
    }
    write_json(args.output, value)


def verify_record(value: dict[str, Any], version: str, commit: str) -> None:
    require(value.get("schema") == "keldra.release-record.v1", "invalid release-record schema")
    validate_identity(value, version, commit, "release record")
    images = value.get("images")
    require(isinstance(images, list), "release record images are missing")
    by_platform: dict[str, dict[str, Any]] = {}
    for item in images:
        require(isinstance(item, dict), "release record image must be an object")
        validate_image(item, version, commit)
        by_platform[item["platform"]] = item
    require(set(by_platform) == set(TARGETS) and len(images) == len(TARGETS), "release record platform set is invalid")
    qualification = value.get("qualification")
    require(isinstance(qualification, dict), "release record qualification is missing")
    require(set(qualification) == {"three_node"}, "release record qualification set is invalid")
    validate_three_node(qualification.get("three_node", {}), version, commit, by_platform["linux/amd64"])
    expected_prerelease = bool(re.search(r"[.-][0-9A-Za-z]", version.split(".", 2)[2]))
    require(value.get("release") == {"prerelease": expected_prerelease, "make_latest": not expected_prerelease}, "release channel does not derive from candidate version")
    packages = value.get("packages")
    require(isinstance(packages, list) and len(packages) == 2, "release package set is invalid")
    expected_packages = [
        {"name": "keldra-api", "version": version, "publication_order": 1},
        {"name": "keldra", "version": version, "publication_order": 2, "requires": f"keldra-api ={version}"},
    ]
    for package, expected in zip(packages, expected_packages, strict=True):
        require(isinstance(package, dict), "release package must be an object")
        require({key: package.get(key) for key in expected} == expected, "package versions or publication order do not match the release")
        require(bool(SHA256.fullmatch(str(package.get("crate_sha256", "")))), "release package crate SHA-256 is invalid")


def verify_command(args: argparse.Namespace) -> None:
    value = read_json(args.record)
    verify_record(value, args.version, args.commit)
    if args.print_field:
        current: Any = value
        for part in args.print_field.split("."):
            require(isinstance(current, dict) and part in current, f"release record has no field {args.print_field}")
            current = current[part]
        require(isinstance(current, (str, int, bool)), "requested release-record field is not scalar")
        print(str(current).lower() if isinstance(current, bool) else current)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    image = commands.add_parser("image")
    for name in ("version", "commit", "platform", "oci-archive", "oci-index-digest", "runnable-manifest-digest", "builder-image", "runtime-image", "server-binary", "cli-binary", "output"):
        image.add_argument("--" + name, required=True)
    image.set_defaults(function=image_command)
    assemble = commands.add_parser("assemble")
    assemble.add_argument("--version", required=True)
    assemble.add_argument("--commit", required=True)
    assemble.add_argument("--image", action="append", required=True)
    assemble.add_argument("--three-node", required=True)
    assemble.add_argument("--package-crate", action="append", required=True)
    assemble.add_argument("--output", required=True)
    assemble.set_defaults(function=assemble_command)
    accept = commands.add_parser("accept-index")
    accept.add_argument("--version", required=True)
    accept.add_argument("--commit", required=True)
    accept.add_argument("--part", action="append", required=True)
    accept.add_argument("--output", required=True)
    accept.set_defaults(function=accept_index_command)
    verify = commands.add_parser("verify")
    verify.add_argument("--record", required=True)
    verify.add_argument("--version", required=True)
    verify.add_argument("--commit", required=True)
    verify.add_argument("--print-field")
    verify.set_defaults(function=verify_command)
    return root


if __name__ == "__main__":
    arguments = parser().parse_args()
    arguments.function(arguments)
