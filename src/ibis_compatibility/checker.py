"""Checker definition."""

from collections import defaultdict
from dataclasses import dataclass
import json
import re

import httpx
from bs4 import BeautifulSoup
from ibis import Expr
import ibis.expr.operations as ops
from ibis.common.graph import Graph

SUPPORTED = "\u2714"
SUPPORT_MATRIX_URL = "https://ibis-project.org/backends/support/matrix"


@dataclass
class CheckResult:
    """Class to display the results of a compatibility check."""

    # The list of backends an expression can be run on.
    backends: list[str]

    # A dictionary mapping operation names to the backends that they cannot be run on.
    restricted_operations: dict[str, list[str]]


class Checker:
    def __init__(self):
        """Constructor."""

        self.backends = None
        self.backend_support = None
        self.initialized = False

    def _initialize(self):
        """Initialize the internal ``backends`` and ``backend_support``.

        Raises:
            RuntimeError: if the ``dt_args`` object can't be found in the HTML.
            RuntimeError: if the table heading can't properly be parsed.
            RuntimeError: if the table data, ``data_json``, isn't present.
        """
        request = httpx.get(SUPPORT_MATRIX_URL)

        try:
            request.raise_for_status()
        except httpx.HTTPStatusError as err:
            raise httpx.HTTPStatusError(
                f"Could not access the support matrix docs page at {SUPPORT_MATRIX_URL}. Got error {err}"
            )

        html_content = request.text
        match = re.search(r"let dt_args = (\{.*?\});", html_content)
        if not match:
            raise RuntimeError(
                "Could not fine ``dt_args`` in the page HTML. "
                "Ensure the docs are loading properly and have not changed recently."
            )

        dt_args = json.loads(match.group(1))

        table_html = dt_args.get("table_html", "")
        soup = BeautifulSoup(table_html, "html.parser")
        thead = soup.find("thead")
        if not thead:
            raise RuntimeError("Could not find table heading in HTML.")

        all_th = [th.get_text(strip=True) for th in thead.find_all("th")]
        backends = all_th[2:22]

        data_json = dt_args.get("data_json")
        if not data_json:
            raise RuntimeError("Could not find ``data_json`` key in data table object.")

        rows = json.loads(data_json)

        backend_support = defaultdict[str, set[str]](set)
        for row in rows:
            op_html = row[1]
            op_soup = BeautifulSoup(op_html, "html.parser")
            operation = op_soup.get_text(strip=True)

            for i, backend in enumerate(backends):
                status_symbol = row[i + 2]
                if status_symbol == SUPPORTED:
                    backend_support[operation].add(backend)

        self.backends = backends
        self.backend_support = backend_support
        self.initialized = True

    def compatible_backends(self, expr: Expr) -> CheckResult:
        """Get a list of backends that support the given expression.

        Args:
            expr: Expression to check compatibility for.

        Returns:
            CheckResult object containing supported backends and restricted operations.
        """
        if not self.initialized:
            self._initialize()

        nodes = Graph.from_bfs(expr.op(), filter=ops.Node).nodes()

        compatible = set(self.backends)
        restricted_operations = defaultdict(list)

        for node in nodes:
            op_name = type(node).__name__
            supported = self.backend_support[op_name]

            removed = compatible - supported

            if removed:
                restricted_operations[op_name].extend(sorted(removed))
                compatible -= removed

        return CheckResult(
            backends=sorted(compatible),
            restricted_operations=dict(restricted_operations),
        )
