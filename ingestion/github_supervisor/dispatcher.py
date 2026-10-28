"""Successor dispatch — design section 4.

Primary (production): GitHubDispatch — a running job dispatches its
successor via the workflow_dispatch API using only its GITHUB_TOKEN
(the documented recursion-guard exemption; PV-1 must prove it in the
synthetic rehearsal before it is relied on). No PAT exists anywhere in
this design (section 4.4).

Lab: RecordingDispatch (captures dispatch requests; the chain driver
acts on them), FailingDispatch (injection), and CallbackDispatch.
"""
import json
import os
import subprocess


class DispatchError(Exception):
    pass


class RecordingDispatch:
    def __init__(self):
        self.calls = []

    def dispatch(self, *, segment_index, manifest_sha256, role="segment"):
        self.calls.append({"segment_index": segment_index,
                           "manifest_sha256": manifest_sha256,
                           "role": role})
        return {"dispatched": True, "via": "recording"}


class FailingDispatch:
    """Raises DispatchError on every call while `always_fail`, else on
    the first `fail_first` calls."""

    def __init__(self, inner=None, fail_first=0, always_fail=False):
        self.inner = inner or RecordingDispatch()
        self.fail_first = fail_first
        self.always_fail = always_fail
        self.attempts = 0

    @property
    def calls(self):
        return self.inner.calls

    def dispatch(self, **kw):
        self.attempts += 1
        if self.always_fail or self.attempts <= self.fail_first:
            raise DispatchError("injected dispatch failure")
        return self.inner.dispatch(**kw)


class GitHubDispatch:
    """workflow_dispatch via the GitHub API with the job's
    GITHUB_TOKEN (env GH_TOKEN/GITHUB_TOKEN). Used only inside Actions
    jobs (rehearsal/production); never from the lab."""

    def __init__(self, repo, workflow_file, ref="main"):
        self.repo = repo                # "owner/name"
        self.workflow_file = workflow_file
        self.ref = ref

    def dispatch(self, *, segment_index, manifest_sha256, role="segment"):
        token = os.environ.get("GH_TOKEN") or os.environ.get(
            "GITHUB_TOKEN")
        if not token:
            raise DispatchError("no GITHUB_TOKEN in environment")
        url = ("https://api.github.com/repos/%s/actions/workflows/%s/"
               "dispatches" % (self.repo, self.workflow_file))
        body = json.dumps({"ref": self.ref, "inputs": {
            "segment_index": str(segment_index),
            "manifest_sha256": manifest_sha256, "role": role}})
        p = subprocess.run(
            ["curl", "-sS", "-X", "POST", url,
             "-H", "Authorization: Bearer %s" % token,
             "-H", "Accept: application/vnd.github+json",
             "-H", "X-GitHub-Api-Version: 2022-11-28",
             "-d", body, "-w", "\n%{http_code}"],
            capture_output=True, text=True, timeout=30)
        code = p.stdout.strip().splitlines()[-1] if p.stdout.strip() \
            else "000"
        if code != "204":
            raise DispatchError("workflow_dispatch HTTP %s: %s"
                                % (code, p.stdout.strip()[:200]))
        return {"dispatched": True, "via": "github-api"}
