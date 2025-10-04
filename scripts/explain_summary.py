"""
Summarize EXPLAIN JSON output into Markdown highlighting index usage,
estimated cost, and row counts.
"""

import json
from pathlib import Path


def extract_plan_info(plan: dict) -> dict:
    """
    Recursively search the plan JSON for Index usage and performance metrics.
    Returns a dict with summary info.
    """
    findings = []

    def _walk(node):
        if isinstance(node, dict):
            ntype = node.get("Node Type", "")
            iname = node.get("Index Name")

            # Collect index info
            if "Index Scan" in ntype or "Bitmap Index Scan" in ntype:
                if iname:
                    findings.append(f"{ntype} on {iname}")
                else:
                    findings.append(ntype)

            # Recurse into children
            for k, v in node.items():
                _walk(v)

    _walk(plan)

    return {
        "indexes": ", ".join(findings) if findings else "No index usage detected",
        "cost": plan.get("Total Cost", plan.get("Startup Cost")),
        "rows": plan.get("Plan Rows"),
    }


def main():
    in_path = Path("data/outputs/explain_plans.json")
    out_path = Path("data/outputs/explain_summary.md")

    if not in_path.exists():
        print(f"❌ Missing input file: {in_path}")
        return

    data = json.loads(in_path.read_text())
    lines = ["# EXPLAIN Summary\n"]

    for entry in data:
        q = entry["query"]
        lines.append(f"## {q}")
        if "error" in entry:
            lines.append(f"- ❌ Error: {entry['error']}\n")
            continue

        # Each plan is usually wrapped in a list
        plan = entry["plan"][0]["Plan"] if isinstance(entry["plan"], list) else entry["plan"]
        info = extract_plan_info(plan)

        lines.append(f"- 📊 Index usage: {info['indexes']}")
        lines.append(f"- 💰 Estimated cost: {info['cost']}")
        lines.append(f"- 📈 Estimated rows: {info['rows']}\n")

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Wrote enhanced summary to {out_path}")


if __name__ == "__main__":
    main()
