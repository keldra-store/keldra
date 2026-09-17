import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("release_record", Path(__file__).parents[1] / "release-record.py")
record = importlib.util.module_from_spec(spec)
spec.loader.exec_module(record)
INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"


class ArchiveAttestationTests(unittest.TestCase):
    def setUp(self):
        self.blobs = {}
        self.runnable = self.blob({"schemaVersion": 2, "mediaType": MANIFEST, "layers": []}, MANIFEST)
        self.attestations = []
        for predicate in ("https://slsa.dev/provenance/v1", "https://spdx.dev/Document"):
            layer = self.blob({"subject": [{"digest": {"sha256": self.runnable["digest"][7:]}}], "predicateType": predicate, "predicate": {"input": "exact-input"}}, "application/vnd.in-toto+json")
            descriptor = self.blob({"schemaVersion": 2, "mediaType": MANIFEST, "layers": [layer]}, MANIFEST)
            descriptor["annotations"] = {"vnd.docker.reference.type": "attestation-manifest", "vnd.docker.reference.digest": self.runnable["digest"]}
            self.attestations.append(descriptor)

    def blob(self, value, media):
        data = json.dumps(value).encode()
        digest = "sha256:" + hashlib.sha256(data).hexdigest()
        self.blobs["blobs/sha256/" + digest[7:]] = data
        return {"mediaType": media, "digest": digest, "size": len(data)}

    def index(self, children):
        return {"schemaVersion": 2, "mediaType": INDEX, "manifests": children}

    def parse(self, index, required=None, duplicate=None):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar"
            with tarfile.open(path, "w") as archive:
                contents = {"index.json": json.dumps(index).encode(), **self.blobs}
                for name, data in contents.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
                if duplicate:
                    data = contents[duplicate]
                    member = tarfile.TarInfo(duplicate)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
            return record.archive_attestations(str(path), self.runnable["digest"], ["exact-input"] if required is None else required)

    def test_direct_and_nested_buildkit_indexes(self):
        children = [self.runnable, *self.attestations]
        direct = self.parse(self.index(children))
        nested = self.blob(self.index(children), INDEX)
        self.assertEqual(self.parse(self.index([nested])), direct)
        self.assertEqual(direct, {"provenance": self.attestations[0]["digest"], "sbom": self.attestations[1]["digest"]})

    def test_nested_digest_corruption(self):
        nested = self.blob(self.index([self.runnable, *self.attestations]), INDEX)
        path = "blobs/sha256/" + nested["digest"][7:]
        self.blobs[path] = self.blobs[path].replace(b"schemaVersion", b"schemaVersioN")
        with self.assertRaisesRegex(SystemExit, "digest changed"):
            self.parse(self.index([nested]))

    def test_nested_size_corruption(self):
        nested = self.blob(self.index([self.runnable]), INDEX)
        nested["size"] += 1
        with self.assertRaisesRegex(SystemExit, "size changed"):
            self.parse(self.index([nested]))

    def test_missing_runnable(self):
        with self.assertRaisesRegex(SystemExit, "runnable subject is missing"):
            self.parse(self.index(self.attestations))

    def test_wrong_attestation_reference(self):
        self.attestations[0]["annotations"]["vnd.docker.reference.digest"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(SystemExit, "another subject"):
            self.parse(self.index([self.runnable, *self.attestations]))

    def test_exact_provenance_inputs_preserved(self):
        with self.assertRaisesRegex(SystemExit, "exact build input"):
            self.parse(self.index([self.runnable, *self.attestations]), ["missing-input"])

    def test_duplicate_tar_members(self):
        with self.assertRaisesRegex(SystemExit, "duplicate member"):
            self.parse(self.index([self.runnable]), duplicate="index.json")

    def test_conflicting_descriptors(self):
        conflicting = dict(self.runnable, mediaType=INDEX)
        with self.assertRaisesRegex(SystemExit, "conflicting descriptors"):
            self.parse(self.index([self.runnable, conflicting]))

    def test_statement_subject_stays_exact(self):
        layer = self.blob({"subject": [{"digest": {"sha256": "0" * 64}}], "predicateType": "https://spdx.dev/Document"}, "application/vnd.in-toto+json")
        descriptor = self.blob({"schemaVersion": 2, "mediaType": MANIFEST, "layers": [layer]}, MANIFEST)
        descriptor["annotations"] = self.attestations[0]["annotations"]
        with self.assertRaisesRegex(SystemExit, "subject changed"):
            self.parse(self.index([self.runnable, descriptor]))

    def test_conflicting_predicate_manifests(self):
        layer = self.blob({"subject": [{"digest": {"sha256": self.runnable["digest"][7:]}}], "predicateType": "https://spdx.dev/Document", "predicate": {"different": True}}, "application/vnd.in-toto+json")
        descriptor = self.blob({"schemaVersion": 2, "mediaType": MANIFEST, "layers": [layer]}, MANIFEST)
        descriptor["annotations"] = self.attestations[0]["annotations"]
        with self.assertRaisesRegex(SystemExit, "conflicting SBOM"):
            self.parse(self.index([self.runnable, *self.attestations, descriptor]))

    def test_excessive_nesting_rejected(self):
        nested = self.runnable
        for _ in range(65):
            nested = self.blob(self.index([nested]), INDEX)
        with self.assertRaisesRegex(SystemExit, "nesting is excessive"):
            self.parse(self.index([nested]))

    def test_cycle_guard_even_with_digest_collision(self):
        # A genuine SHA-256 cycle is infeasible; force a collision to exercise
        # the explicit traversal guard independently of that assumption.
        descriptor = {"mediaType": INDEX, "digest": "sha256:" + "0" * 64, "size": 0}
        for _ in range(3):
            descriptor["size"] = len(json.dumps(self.index([descriptor])).encode())
        self.blobs["blobs/sha256/" + "0" * 64] = json.dumps(self.index([descriptor])).encode()
        with mock.patch.object(record.hashlib, "sha256") as digest:
            digest.return_value.hexdigest.return_value = "0" * 64
            with self.assertRaisesRegex(SystemExit, "cycle detected"):
                self.parse(self.index([descriptor]))


if __name__ == "__main__":
    unittest.main()
