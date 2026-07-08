"""End-to-end test for the mkdocs-rangefind plugin.

Runs a real `mkdocs build` against a fixture site, then asserts that the
Rangefind index and widget assets were produced and the widget tags injected.
Finally it runs a companion Node script that serves the built site over a
Range-capable HTTP server and queries the real runtime, proving the index is
searchable.
"""

import json
import os
import shutil
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(HERE, "fixture")
# integrations/mkdocs-rangefind/test -> repo root is three levels up.
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
SITE_DIR = os.path.join(FIXTURE, "site")


def _link(target, link_path):
    if os.path.islink(link_path) or os.path.exists(link_path):
        if os.path.islink(link_path):
            os.unlink(link_path)
        elif os.path.isdir(link_path):
            shutil.rmtree(link_path)
        else:
            os.remove(link_path)
    os.symlink(target, link_path)


class RangefindMkDocsBuildTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Make the local repo checkout resolvable as the `rangefind` npm package
        # from the fixture dir, so `npx rangefind` and Node's require.resolve
        # work offline (mirrors what `npm install rangefind` would produce).
        node_modules = os.path.join(FIXTURE, "node_modules")
        os.makedirs(os.path.join(node_modules, ".bin"), exist_ok=True)
        _link(REPO_ROOT, os.path.join(node_modules, "rangefind"))
        _link(
            os.path.join("..", "rangefind", "bin", "rangefind.js"),
            os.path.join(node_modules, ".bin", "rangefind"),
        )

        if os.path.isdir(SITE_DIR):
            shutil.rmtree(SITE_DIR)

        # Run the real MkDocs build from within the venv.
        result = subprocess.run(
            [sys.executable, "-m", "mkdocs", "build", "--strict"],
            cwd=FIXTURE,
            capture_output=True,
            text=True,
            timeout=120,
        )
        cls.build = result
        if result.returncode != 0:
            raise AssertionError(
                "mkdocs build failed:\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )

    def test_build_succeeded(self):
        self.assertEqual(self.build.returncode, 0, self.build.stderr)

    def test_manifest_exists_and_indexed_all_pages(self):
        manifest_path = os.path.join(SITE_DIR, "rangefind", "manifest.min.json")
        self.assertTrue(os.path.isfile(manifest_path), manifest_path)
        with open(manifest_path, encoding="utf-8") as handle:
            manifest = json.load(handle)
        self.assertGreaterEqual(
            manifest.get("total", 0), 2, f"expected >=2 docs, manifest={manifest!r}"
        )

    def test_widget_assets_copied(self):
        assets = os.path.join(SITE_DIR, "_rangefind")
        self.assertTrue(os.path.isfile(os.path.join(assets, "rangefind-search.js")))
        self.assertTrue(os.path.isfile(os.path.join(assets, "rangefind-search.css")))

    def test_page_html_has_injected_tags(self):
        index_html = os.path.join(SITE_DIR, "index.html")
        with open(index_html, encoding="utf-8") as handle:
            html = handle.read()
        self.assertIn('<script type="module"', html)
        self.assertIn("rangefind-search.js", html)
        self.assertIn("<rangefind-search", html)
        # theme: true in the fixture -> stylesheet link injected too.
        self.assertIn("rangefind-search.css", html)

    def test_nested_relative_paths(self):
        # The About page lives at site/about/index.html (use_directory_urls),
        # so its asset references must be one level up.
        about_html = os.path.join(SITE_DIR, "about", "index.html")
        with open(about_html, encoding="utf-8") as handle:
            html = handle.read()
        self.assertIn("../_rangefind/rangefind-search.js", html)
        self.assertIn('src="../rangefind/"', html)

    def test_index_is_searchable(self):
        script = os.path.join(HERE, "verify_search.mjs")
        result = subprocess.run(
            ["node", script, SITE_DIR, "rangefind", "xylophone"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(
            result.returncode,
            0,
            f"search verification failed:\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}",
        )
        self.assertIn("resolved to the About page", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
