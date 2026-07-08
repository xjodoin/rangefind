"""Rangefind MkDocs plugin.

MkDocs is a Python static-site generator; Rangefind is a Node.js tool. There is
no way to run Rangefind's indexing logic from pure Python, so this plugin does
the honest thing: after MkDocs finishes building the site it *shells out* to the
Rangefind Node CLI (``npx rangefind build <site_dir>``) to crawl the freshly
rendered HTML into a static, range-request search index, then copies the
Rangefind search Web Component assets into the site and injects the widget tags
into every rendered page.

Prerequisite: Node.js and the ``rangefind`` npm package must be reachable from
the MkDocs project directory (typically ``npm install rangefind`` so that
``npx rangefind`` and Node's module resolution both find it).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from urllib.parse import urlsplit

from mkdocs.config import config_options
from mkdocs.exceptions import PluginError
from mkdocs.plugins import BasePlugin
from mkdocs.utils import get_relative_url

try:  # MkDocs >= 1.2 ships a namespaced logger helper.
    from mkdocs.plugins import get_plugin_logger

    log = get_plugin_logger(__name__)
except ImportError:  # pragma: no cover - very old MkDocs
    import logging

    log = logging.getLogger(f"mkdocs.plugins.{__name__}")


class RangefindPlugin(BasePlugin):
    """Build a Rangefind index after the site is built and inject the widget."""

    # MkDocs uses the classic tuple-based config scheme (name, option) pairs.
    config_scheme = (
        # Master switch. When false the plugin is a complete no-op.
        ("enabled", config_options.Type(bool, default=True)),
        # URL prefix baked into the *result links* stored in the index
        # (passed to the CLI as --base-url). Empty string => derive from the
        # MkDocs `site_url` path (falling back to "/"). Set explicitly when the
        # index's stored URLs need a specific prefix.
        ("base_url", config_options.Type(str, default="")),
        # Index output directory, relative to the built site_dir.
        ("output_dir", config_options.Type(str, default="rangefind")),
        # Where the Web Component JS/CSS assets are copied, relative to site_dir.
        ("assets_dir", config_options.Type(str, default="_rangefind")),
        # Include the optional framework-free theme stylesheet. Off by default:
        # the component is headless and inherits the host page's CSS.
        ("theme", config_options.Type(bool, default=False)),
        # Command used to run the Rangefind CLI. "npx" resolves the local
        # `rangefind` install; can be pointed at a specific binary in CI.
        ("node_command", config_options.Type(str, default="npx")),
        # Where to place the <rangefind-search> element:
        #   "body_end" (default) - injected right before </body> on every page.
        #   "manual"             - not injected; you hand-place it in a theme
        #                          override. Script/CSS are still injected.
        ("placement", config_options.Choice(("body_end", "manual"), default="body_end")),
        # Optional literal HTML marker. When set (and placement != manual), the
        # <rangefind-search> element is inserted immediately after the first
        # occurrence of this exact substring instead of before </body> (e.g.
        # '<div id="rangefind">'). Falls back to body_end if not found.
        ("selector", config_options.Type(str, default="")),
        # Input placeholder text for the injected element.
        ("placeholder", config_options.Type(str, default="Search")),
        # Arbitrary extra attributes for the injected <rangefind-search> element
        # (e.g. {hotkey: true, router: true, input-class: "w-full"}).
        ("element_attributes", config_options.Type(dict, default={})),
        # Explicit paths to the component assets. When unset the plugin asks
        # Node to resolve them from the installed `rangefind` package.
        ("element_js_path", config_options.Type(str, default="")),
        ("element_css_path", config_options.Type(str, default="")),
    )

    # ---- lifecycle hooks -------------------------------------------------

    def on_post_build(self, config, **kwargs):
        """Real MkDocs post-build hook: crawl the built site and copy assets."""
        if not self.config["enabled"]:
            return

        site_dir = config["site_dir"]
        project_dir = _project_dir(config)
        output_abs = os.path.join(site_dir, self.config["output_dir"])
        base_url = self._resolve_base_url(config)

        # 1. Build the index by shelling out to the Node CLI. Real flags,
        #    confirmed against bin/rangefind.js:
        #      rangefind build <dir> --output <dir> --base-url <url>
        command = [
            self.config["node_command"],
            "rangefind",
            "build",
            site_dir,
            "--output",
            output_abs,
            "--base-url",
            base_url,
        ]
        log.info("Building Rangefind index: %s", " ".join(command))
        try:
            result = subprocess.run(
                command,
                cwd=project_dir,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            raise PluginError(
                f"mkdocs-rangefind: could not run '{self.config['node_command']}'. "
                "Node.js and the `rangefind` npm package must be installed and on "
                f"PATH. Original error: {exc}"
            )
        if result.returncode != 0:
            raise PluginError(
                "mkdocs-rangefind: `rangefind build` failed "
                f"(exit code {result.returncode}).\n"
                f"command: {' '.join(command)}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
        if result.stdout.strip():
            log.info(result.stdout.strip())

        # 2. Copy the Web Component assets into the site.
        js_src, css_src = self._resolve_assets(project_dir)
        assets_abs = os.path.join(site_dir, self.config["assets_dir"])
        os.makedirs(assets_abs, exist_ok=True)
        shutil.copyfile(js_src, os.path.join(assets_abs, "rangefind-search.js"))
        if self.config["theme"]:
            shutil.copyfile(css_src, os.path.join(assets_abs, "rangefind-search.css"))
        log.info("Copied Rangefind widget assets to %s", assets_abs)

    def on_post_page(self, output, page, config, **kwargs):
        """Real MkDocs post-page hook: inject the widget into the page HTML."""
        if not self.config["enabled"]:
            return output

        assets_dir = self.config["assets_dir"].strip("/")
        output_dir = self.config["output_dir"].strip("/")
        page_url = page.url

        js_href = get_relative_url(f"{assets_dir}/rangefind-search.js", page_url)
        index_src = get_relative_url(f"{output_dir}/", page_url)

        head_injection = f'<script type="module" src="{_attr(js_href)}"></script>'
        if self.config["theme"]:
            css_href = get_relative_url(f"{assets_dir}/rangefind-search.css", page_url)
            head_injection = (
                f'<link rel="stylesheet" href="{_attr(css_href)}">\n' + head_injection
            )
        output = _inject_head(output, head_injection)

        if self.config["placement"] != "manual":
            element = self._render_element(index_src)
            output = self._inject_element(output, element)

        return output

    # ---- helpers ---------------------------------------------------------

    def _resolve_base_url(self, config):
        configured = self.config["base_url"].strip()
        if configured:
            return configured
        site_url = config.get("site_url")
        if site_url:
            path = urlsplit(site_url).path or "/"
            return path if path.endswith("/") else path + "/"
        return "/"

    def _render_element(self, index_src):
        attrs = {"src": index_src, "placeholder": self.config["placeholder"]}
        for key, value in (self.config["element_attributes"] or {}).items():
            if value is True:
                attrs[str(key)] = ""  # bare boolean attribute
            elif value is False or value is None:
                continue
            else:
                attrs[str(key)] = str(value)
        rendered = []
        for key, value in attrs.items():
            rendered.append(key if value == "" else f'{key}="{_attr(value)}"')
        return f"<rangefind-search {' '.join(rendered)}></rangefind-search>"

    def _inject_element(self, output, element):
        selector = self.config["selector"]
        if selector:
            idx = output.find(selector)
            if idx != -1:
                cut = idx + len(selector)
                return output[:cut] + "\n" + element + output[cut:]
            log.warning(
                "mkdocs-rangefind: selector %r not found on a page; "
                "falling back to placement before </body>.",
                selector,
            )
        return _inject_body_end(output, element)

    def _resolve_assets(self, project_dir):
        """Locate the component's JS/CSS.

        Preference order:
          1. Explicit `element_js_path` / `element_css_path` config values.
          2. Ask Node to resolve them via the `rangefind` package export map
             (`rangefind/element` and `rangefind/element.css`). The export map
             is the robust seam here: `require.resolve('rangefind/package.json')`
             is blocked by the package's `exports`, but the declared subpath
             exports point straight at dist/rangefind-search.{js,css}.
        """
        js_cfg = self.config["element_js_path"].strip()
        css_cfg = self.config["element_css_path"].strip()
        if js_cfg and css_cfg:
            return _require_file(js_cfg, project_dir), _require_file(css_cfg, project_dir)

        node_bin = _node_binary(self.config["node_command"])
        js = js_cfg or self._node_resolve(node_bin, "rangefind/element", project_dir)
        css = css_cfg or self._node_resolve(node_bin, "rangefind/element.css", project_dir)
        return _require_file(js, project_dir), _require_file(css, project_dir)

    def _node_resolve(self, node_bin, specifier, project_dir):
        code = f"process.stdout.write(require.resolve({specifier!r}))"
        try:
            result = subprocess.run(
                [node_bin, "-e", code],
                cwd=project_dir,
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            raise PluginError(
                f"mkdocs-rangefind: could not run Node ('{node_bin}') to resolve "
                f"the '{specifier}' asset. Install Node.js, or set "
                "`element_js_path`/`element_css_path` explicitly in mkdocs.yml. "
                f"Original error: {exc}"
            )
        path = result.stdout.strip()
        if result.returncode != 0 or not path:
            raise PluginError(
                f"mkdocs-rangefind: could not resolve '{specifier}' from the "
                f"`rangefind` npm package (run from {project_dir}). Make sure "
                "`rangefind` is installed there (e.g. `npm install rangefind`), "
                "or set `element_js_path`/`element_css_path` in mkdocs.yml.\n"
                f"stderr:\n{result.stderr}"
            )
        return path


# ---- module-level pure helpers ------------------------------------------


def _project_dir(config):
    config_file = config.get("config_file_path")
    if config_file:
        return os.path.dirname(os.path.abspath(config_file))
    return os.getcwd()


def _node_binary(node_command):
    """Derive a plain Node binary from the configured command.

    The index is built with `node_command` (default "npx"), but resolving the
    asset paths needs Node itself. If the command is npx, use the sibling
    `node`; otherwise assume the command can already run `-e`.
    """
    base = os.path.basename(node_command)
    if base in ("npx", "npx.cmd", "npx.exe"):
        directory = os.path.dirname(node_command)
        return os.path.join(directory, "node") if directory else "node"
    return node_command


def _require_file(path, project_dir):
    resolved = path if os.path.isabs(path) else os.path.join(project_dir, path)
    if not os.path.isfile(resolved):
        raise PluginError(f"mkdocs-rangefind: asset file not found: {resolved}")
    return resolved


def _attr(value):
    return str(value).replace("&", "&amp;").replace('"', "&quot;")


def _inject_head(output, snippet):
    lower = output.lower()
    idx = lower.rfind("</head>")
    if idx != -1:
        return output[:idx] + snippet + "\n" + output[idx:]
    return _inject_body_end(output, snippet)


def _inject_body_end(output, snippet):
    lower = output.lower()
    idx = lower.rfind("</body>")
    if idx != -1:
        return output[:idx] + snippet + "\n" + output[idx:]
    return output + "\n" + snippet
